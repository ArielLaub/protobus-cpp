// A battle royale played over protobus: six players, each its own instance of
// the Combat.Player service (Combat.Player.player1 ... player6) with its own
// strategy. Players shoot each other by RPC; joins, hits, deaths, turns and
// the winner travel as events every player subscribes to.
//
//   docker compose up -d --wait
//   AMQP_URL=amqp://guest:guest@127.0.0.1:25672/ ./build/examples/combat
//
// A port of the TypeScript, Python and Go combat samples, on the same schema:
// it plays against their players unchanged.
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <future>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <sstream>
#include <thread>

#include "player.protobus.h"

namespace {

constexpr int kStartingHealth = 10;

std::mutex printMutex;
void say(const std::string& line) {
  std::lock_guard<std::mutex> lock(printMutex);
  std::cout << line << std::endl;
}

struct Opponent {
  std::string id;
  std::string name;
  int health = kStartingHealth;
  bool alive = true;
};

// What a strategy may read and keep.
struct View {
  int health = kStartingHealth;
  std::string lastAttacker;
  std::string focus;  // strategies that hold a grudge store it here
  std::mt19937_64 rand;
};

using Strategy = std::function<std::optional<Opponent>(View&, const std::vector<Opponent>&)>;

std::optional<Opponent> random(View& v, const std::vector<Opponent>& alive) {
  if (alive.empty()) return std::nullopt;
  return alive[std::uniform_int_distribution<size_t>(0, alive.size() - 1)(v.rand)];
}

template <typename Better>
std::optional<Opponent> pick(const std::vector<Opponent>& alive, Better better) {
  if (alive.empty()) return std::nullopt;
  Opponent best = alive.front();
  for (const auto& o : alive) {
    if (better(o, best)) best = o;
  }
  return best;
}

// The six strategies of the original sample.
const std::vector<std::pair<std::string, Strategy>>& strategies() {
  static const std::vector<std::pair<std::string, Strategy>> s = {
      {"The Vindicator",
       [](View& v, const std::vector<Opponent>& alive) -> std::optional<Opponent> {
         for (const auto& o : alive) {
           if (o.id == v.lastAttacker) return o;
         }
         return random(v, alive);
       }},
      {"The Bully Hunter",
       [](View&, const std::vector<Opponent>& alive) {
         return pick(alive, [](const Opponent& a, const Opponent& b) { return a.health < b.health; });
       }},
      {"The Giant Slayer",
       [](View&, const std::vector<Opponent>& alive) {
         return pick(alive, [](const Opponent& a, const Opponent& b) { return a.health > b.health; });
       }},
      {"The Equalizer",
       [](View& v, const std::vector<Opponent>& alive) {
         return pick(alive, [&v](const Opponent& a, const Opponent& b) {
           return std::abs(a.health - v.health) < std::abs(b.health - v.health);
         });
       }},
      {"The Wildcard", [](View& v, const std::vector<Opponent>& alive) { return random(v, alive); }},
      {"The Terminator",
       [](View& v, const std::vector<Opponent>& alive) -> std::optional<Opponent> {
         for (const auto& o : alive) {
           if (o.id == v.focus) return o;
         }
         auto o = random(v, alive);
         if (o) v.focus = o->id;
         return o;
       }},
  };
  return s;
}

// One contestant: an instance of Combat.Player. Requests and events are
// separate consumers, so handlers run concurrently and the state is guarded.
class Player : public Combat::PlayerBase {
 public:
  Player(protobus::Context& ctx, std::string id, std::string name, Strategy strategy, uint64_t seed)
      : PlayerBase(ctx), id_(std::move(id)), name_(std::move(name)), strategy_(std::move(strategy)) {
    view_.rand.seed(seed);
  }

  // Each player is an instance of the one Combat.Player contract.
  std::string ServiceName() const override { return "Combat.Player." + id_; }
  const std::string& id() const { return id_; }
  const std::string& name() const { return name_; }

  Combat::ShootResponse shoot(const Combat::ShootRequest& request, protobus::CallContext&) override {
    bool hit;
    int health;
    std::string shooter = request.shooterid();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (view_.health <= 0) {
        Combat::ShootResponse dead;
        dead.set_hit(false);
        return dead;
      }
      hit = std::uniform_int_distribution<int>(0, 1)(view_.rand) == 0;
      if (hit) {
        --view_.health;
        view_.lastAttacker = request.shooterid();
      }
      health = view_.health;
      if (auto it = others_.find(shooter); it != others_.end()) shooter = it->second.name;
    }
    say(hit ? "  " + name_ + " was hit by " + shooter + "! Health: " + std::to_string(health)
            : "  " + name_ + " dodged an attack from " + shooter + "!");
    Combat::PlayerShot shot;
    shot.set_shooterid(request.shooterid());
    shot.set_targetid(id_);
    shot.set_hit(hit);
    shot.set_targethealth(health);
    publishEvent(shot);
    if (hit && health <= 0) {
      say("  " + name_ + " has been eliminated!");
      Combat::PlayerDied died;
      died.set_playerid(id_);
      died.set_killedby(request.shooterid());
      publishEvent(died);
    }
    Combat::ShootResponse response;
    response.set_hit(hit);
    response.set_remaininghealth(health);
    return response;
  }

  // The turn order. The first player takes its turn straight away, on its own
  // thread, so the RPC answers at once.
  Combat::InitiateGameResponse initiateGame(const Combat::InitiateGameRequest& request,
                                            protobus::CallContext&) override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      order_.assign(request.playerorder().begin(), request.playerorder().end());
    }
    if (request.myindex() == 0) {
      auto self = std::static_pointer_cast<Player>(shared_from_this());
      std::thread([self] { self->takeTurn(); }).detach();
    }
    Combat::InitiateGameResponse response;
    response.set_success(true);
    return response;
  }

  Combat::GetStatusResponse getStatus(const Combat::GetStatusRequest&, protobus::CallContext&) override {
    std::lock_guard<std::mutex> lock(mutex_);
    Combat::GetStatusResponse status;
    status.set_playerid(id_);
    status.set_playername(name_);
    status.set_health(view_.health);
    status.set_alive(view_.health > 0);
    return status;
  }

  void meet(const std::string& id, const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (id != id_) others_[id] = Opponent{id, name};
  }

  void subscribe() {
    subscribeEvent<Combat::PlayerJoined>(
        [this](const Combat::PlayerJoined& e, const std::string&, const std::string&) {
          meet(e.playerid(), e.playername());
        });
    subscribeEvent<Combat::PlayerShot>([this](const Combat::PlayerShot& e, const std::string&, const std::string&) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (auto it = others_.find(e.targetid()); it != others_.end()) {
        it->second.health = e.targethealth();
        it->second.alive = e.targethealth() > 0;
      }
    });
    subscribeEvent<Combat::PlayerDied>([this](const Combat::PlayerDied& e, const std::string&, const std::string&) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (auto it = others_.find(e.playerid()); it != others_.end()) {
        it->second.health = 0;
        it->second.alive = false;
      }
      if (view_.focus == e.playerid()) view_.focus.clear();
    });
    subscribeEvent<Combat::TurnComplete>(
        [this](const Combat::TurnComplete& e, const std::string&, const std::string&) {
          bool mine;
          {
            std::lock_guard<std::mutex> lock(mutex_);
            const auto at = std::find(order_.begin(), order_.end(), id_);
            mine = at != order_.end() && at - order_.begin() == e.nextplayerindex();
          }
          if (mine) takeTurn();
        });
    subscribeEvent<Combat::GameOver>([this](const Combat::GameOver&, const std::string&, const std::string&) {
      std::lock_guard<std::mutex> lock(mutex_);
      gameOver_ = true;
    });
  }

 private:
  std::vector<Opponent> aliveOthers() const {
    std::vector<Opponent> alive;
    for (const auto& [_, o] : others_) {
      if (o.alive) alive.push_back(o);
    }
    return alive;
  }

  void takeTurn() {
    std::optional<Opponent> target;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (gameOver_) return;
      if (view_.health > 0) {
        auto alive = aliveOthers();
        if (alive.empty()) {
          win();
          return;
        }
        target = strategy_(view_, alive);
      }
    }
    // A turn handed to a player who died meanwhile is passed on, not
    // dropped: dropping it would stall the game.
    if (target) {
      say("  " + name_ + " shoots at " + target->name + "!");
      Combat::PlayerProxy victim(context(), "Combat.Player." + target->id);
      victim.init();
      Combat::ShootRequest shot;
      shot.set_shooterid(id_);
      protobus::CallOptions options;
      options.actor = id_;
      try {
        const auto result = victim.shoot(shot, options);
        if (result.remaininghealth() <= 0) {
          std::lock_guard<std::mutex> lock(mutex_);
          others_[target->id].alive = false;
          others_[target->id].health = 0;
        }
      } catch (const std::exception& e) {
        say("  " + name_ + " failed to shoot: " + e.what());
      }
      std::lock_guard<std::mutex> lock(mutex_);
      if (aliveOthers().empty()) {
        win();
        return;
      }
    }
    endTurn();
  }

  void win() {
    say("  " + name_ + " is the last one standing!");
    Combat::GameOver over;
    over.set_winnerid(id_);
    over.set_winnername(name_);
    publishEvent(over);
  }

  void endTurn() {
    int32_t next;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto me = std::find(order_.begin(), order_.end(), id_) - order_.begin();
      const auto n = static_cast<long>(order_.size());
      next = static_cast<int32_t>((me + 1) % n);
      for (long i = 1; i <= n; ++i) {
        const long idx = (me + i) % n;
        auto it = others_.find(order_[idx]);
        if (it != others_.end() && it->second.alive) {
          next = static_cast<int32_t>(idx);
          break;
        }
      }
    }
    Combat::TurnComplete turn;
    turn.set_playerid(id_);
    turn.set_nextplayerindex(next);
    publishEvent(turn);
  }

  std::string id_;
  std::string name_;
  Strategy strategy_;
  std::mutex mutex_;
  View view_;
  std::map<std::string, Opponent> others_;
  std::vector<std::string> order_;
  bool gameOver_ = false;
};

}  // namespace

int main() {
  // The game narrates itself; the framework's own lines would drown it out.
  protobus::setLogLevel(protobus::LogLevel::Warn);
  const char* url = std::getenv("AMQP_URL");
  protobus::Context context;
  context.init(url ? url : "amqp://guest:guest@127.0.0.1:25672/");
  const std::string rule(60, '=');
  say(rule + "\nCOMBAT GAME - Battle Royale!\n" + rule);

  // Hear the result like any other subscriber.
  auto results = std::make_shared<protobus::EventListener>(context.connectionPtr(), context.factoryPtr());
  results->init(nullptr, "");
  std::promise<std::string> winner;
  std::once_flag announced;
  results->subscribe<Combat::GameOver>([&](const Combat::GameOver& e, const std::string&, const std::string&) {
    std::call_once(announced, [&] { winner.set_value(e.winnername()); });
  });
  results->start();

  const uint64_t seed = static_cast<uint64_t>(std::chrono::system_clock::now().time_since_epoch().count());
  std::vector<std::shared_ptr<Player>> players;
  std::vector<std::string> order;
  for (size_t i = 0; i < strategies().size(); ++i) {
    const std::string id = "player" + std::to_string(i + 1);
    auto p = std::make_shared<Player>(context, id, strategies()[i].first, strategies()[i].second, seed + i);
    p->init();
    p->subscribe();
    players.push_back(p);
    order.push_back(id);
    say("  joined: " + p->name() + " (" + id + ")");
  }
  for (const auto& p : players) {
    for (const auto& o : players) p->meet(o->id(), o->name());
    Combat::PlayerJoined joined;
    joined.set_playerid(p->id());
    joined.set_playername(p->name());
    joined.set_health(kStartingHealth);
    context.publishEvent(joined);
  }
  std::string turnOrder;
  for (const auto& id : order) turnOrder += (turnOrder.empty() ? "" : " -> ") + id;
  say("Turn order: " + turnOrder + "\n" + rule + "\nLET THE BATTLE BEGIN!\n" + rule);

  // Index 0 is initiated last: its first turn passes the turn on, and every
  // other player must know the order by then.
  for (int i = static_cast<int>(order.size()) - 1; i >= 0; --i) {
    Combat::PlayerProxy player(context, "Combat.Player." + order[i]);
    player.init();
    Combat::InitiateGameRequest request;
    for (const auto& id : order) request.add_playerorder(id);
    request.set_myindex(i);
    player.initiateGame(request);
  }

  auto done = winner.get_future();
  if (done.wait_for(std::chrono::minutes(2)) != std::future_status::ready) {
    std::cerr << "the game did not finish" << std::endl;
    return 1;
  }
  done.get();

  say(rule + "\nFINAL RESULTS\n" + rule);
  for (const auto& id : order) {
    Combat::PlayerProxy player(context, "Combat.Player." + id);
    player.init();
    const auto status = player.getStatus({});
    std::ostringstream line;
    line << "  " << std::left << std::setw(18) << status.playername() << std::right << std::setw(2)
         << status.health() << " HP  (" << (status.alive() ? "WINNER" : "eliminated") << ")";
    say(line.str());
  }
  for (auto& p : players) p->stopConsuming();
  context.connection().drainInFlight(5000);
  return 0;
}
