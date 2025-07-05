//////////////////////////////////////////////////////////////////////////////////////
//      👑BLACKJACK DEMO v0.1👑
// inspired by: https://codereview.stackexchange.com/questions/82103/ascii-fication-of-playing-cards
// include('./demoes/language/BlackJack.sql')
// ┌─────────┐ ┌─────────┐
// │ A    ♥️ │ │ K    ♦️ │
// │         │ │         │
// │   ♥️    │ │   ♦️    │
// │         │ │         │
// │ ♥️    A │ │ ♦️    K │
// └─────────┘ └─────────┘
//////////////////////////////////////////////////////////////////////////////////////

use agg, oxide, util

//////////////////////////////////////////////////////////////////////////////////////
//      GLOBAL STATE
//////////////////////////////////////////////////////////////////////////////////////

// define a type to represent a card deck/hand
Cards = Table(face: String(2), suit: String(2))

// create the card faces & suits
faces = select face: value from ((2..=10)::map(n -> n::to_string()) ++ ["J", "Q", "K", "A"])::to_table()
suits = select suit: value from ["♥️", "♦️", "♣️", "♠️"]::to_table()

// create the deck, player-hand and dealer-hand
deck = nsd::save("games.blackjack.deck", Cards::new())
player = nsd::save("games.blackjack.player", Cards::new())
dealer = nsd::save("games.blackjack.dealer", Cards::new())

//////////////////////////////////////////////////////////////////////////////////////
//      UTILITY METHODS
//////////////////////////////////////////////////////////////////////////////////////

fn show_title() -> {
  println(
        "| 👑BLACKJACK DEMO v0.1👑
         |        _             _                                 _
         |  _ __ | | __ _ _   _(_)_ __   __ _    ___ __ _ _ __ __| |___
         | | '_ \| |/ _` | | | | | '_ \ / _` |  / __/ _` | '__/ _` / __|
         | | |_) | | (_| | |_| | | | | | (_| | | (_| (_| | | | (_| \__ \
         | | .__/|_|\__,_|\__, |_|_| |_|\__, |  \___\__,_|_|  \__,_|___/
         | |_|            |___/         |___/
         |"::strip_margin('|'))
}

fn card_face_up(face, suit) -> {
    let faceL = if(face::len() < 2, face + " ", face)
    let faceR = if(face::len() < 2, " " + face, face);
    [
        "┌─────────┐",
sprintf("│ %s   %s │", faceL, suit),
        "│         │",
sprintf("│   %s    │", suit),
        "│         │",
sprintf("│ %s   %s │", suit, faceR),
        "└─────────┘"
    ]
}

fn card_face_down() -> [
    "┌─────────┐",
    "│▒░▒░▒░▒░▒│",
    "│░▒░▒░▒░▒░│",
    "│▒░▒░▒░▒░▒│",
    "│░▒░▒░▒░▒░│",
    "│▒░▒░▒░▒░▒│",
    "└─────────┘"
]

fn compute_score(hand, aceScore: i64) -> {
    let rows =
        select
            score: sum(match face {
                "A" => aceScore
                face when face:::to(i64) in 2..9 => face:::to(i64)
                _ => 10
            })
        from hand;
    let row = rows[0];
    row.score
}

fn compute_card_score(hand) -> {
    let v11 = compute_score(hand, 11)
    if(v11 <= 21, v11, compute_score(hand, 1))
}

fn hit(hand) -> {
    deck::shuffle()
    card <~ deck
    card ~> hand
}

fn show_hand(hand) -> {
    let lines = []
    for card in hand {
        let _card = card_face_up(card.face, card.suit)
        lines = if(lines::is_empty, _card, (lines <|> _card)::map(a -> a::to_array::join(" ")))
    }

    // add a face down card in the last position
    if (hand == dealer) {
        _card = card_face_down()
        lines = if(lines::is_empty, _card, (lines <|> _card)::map(a -> a::to_array::join(" ")))
    }

    // write to STDOUT
    println((lines::join('\n')) + '\n')
}

fn create_gamestate() -> {
    {
        name: (__realname__::split(" "))[0],
        money: 1000.0,
        bet: 25.0,
        bet_factor: 1.0,
        dealer_score: 0,
        player_score: 0,
        level: 1,
        loop: 0,
        is_jousting: true,
        is_alive: true,
        show_cards: false
    }
}

fn game_over(gamestate) -> {
    update_gamestate(gamestate)
    if (gamestate.dealer_score == gamestate.player_score)
        round_completed(gamestate, '🤝Draw.', 0)
    else if (gamestate.player_score == 21)
        round_completed(gamestate, '👍You Win!! - Player BlackJack!', 1.5 * bet)
    else if (gamestate.dealer_score == 21)
        round_completed(gamestate, '👎You Lose - Dealer BlackJack!', -bet)
    else if (gamestate.dealer_score > 21)
        round_completed(gamestate, sprintf('👍You Win!! - Dealer Busts: %d', gamestate.dealer_score), bet)
    else if (gamestate.player_score > 21)
        round_completed(gamestate, sprintf('👎You Lose - Player Busts: %d', gamestate.player_score), -bet)
    else if (gamestate.player_score > gamestate.dealer_score)
        round_completed(gamestate, sprintf('👍You Win!! - %d vs. %d', gamestate.player_score, gamestate.dealer_score), -bet)
    else round_completed(gamestate, sprintf('👎You Lose - %d vs. %d', gamestate.dealer_score, gamestate.player_score), -bet)
    gamestate.level += 1
}

fn update_gamestate(gamestate) -> {
    gamestate.dealer_score = (compute_card_score(dealer) ? 0)
    gamestate.player_score = (compute_card_score(player) ? 0)
    gamestate
}

fn prompt_user(gamestate) -> {
    println(sprintf("Choose %s[H]it, [S]tand or [Q]uit? ", if(gamestate.loop == 0, "[D]ouble-down, ", "")))
    choice = io::stdin()::trim::to_uppercase
    if ((choice::starts_with("D") is true) && (loop == 0)) gamestate.bet_factor = 2.0
    else if(choice::starts_with("H")) { hit(player); gamestate.show_cards = true }
    else if(choice::starts_with("Q")) { gamestate.is_jousting = false; gamestate.is_alive = false }
    else if(choice::starts_with("S")) gamestate.is_jousting = false
}

fn dealer_intelligence(gamestate, finish) -> {
    update_gamestate(gamestate)
    let modified = false
    if((gamestate.player_score <= 21) && (gamestate.dealer_score < gamestate.player_score)) {
        let cost =
            if (finish) while (gamestate.player_score > gamestate.dealer_score) hit(dealer)
            else if(gamestate.player_score > gamestate.dealer_score) hit(dealer)
        modified = modified || (cost.inserted > 0)
    }
    modified
}

fn round_completed(gamestate, message: String, betDelta: f64) -> {
    println(sprintf('%s\n', message))
    gamestate.money += gamestate.bet_factor * betDelta
}

fn show_game_table(gamestate) -> {
    println(sprintf("DEALER - %d/21", gamestate.dealer_score))
    show_hand(dealer)
    println(sprintf("YOU (%s) - %s%d/21",
        gamestate.name,
        if(gamestate.bet_factor == 2.0, "2x ", ""),
        gamestate.player_score))
    show_hand(player)
}

fn show_separator(gamestate) -> {
    separator = ("※" * 120) + '\n'
    println('\n' + separator)
    println(sprintf(" Player: %s \t Credit: $%.2f \t Bet: $%.2f \t Round: %d \n",
         gamestate.name, gamestate.money, gamestate.bet, gamestate.level))
    println(separator + '\n')
}

//////////////////////////////////////////////////////////////////////////////////////
//      MAIN PROGRAM
//////////////////////////////////////////////////////////////////////////////////////

fn main() -> {
    show_title()

    // setup the basic game state
    gamestate = create_gamestate()
    run(gamestate)
}

fn run(gamestate) -> {                                                                                          v
    if gamestate.is_alive {
        play_game(gamestate)
        run(gamestate)
    }
}

fn play_game(gamestate) -> {                                                                                          fn run(gamestate) -> {
    // reset and shuffle the deck
    (faces * suits)::to_table ~>> deck

    // put some cards into the hands of the player and dealer
    for hand in [dealer, player] {
        hand::resize(0)
        hit(hand)
    }

    gamestate.is_jousting = true
    gamestate.bet_factor = 1.0

    // display the hands of the player and dealer
    show_separator(gamestate)
    show_game_table(gamestate)

    // main loop
    gamestate.loop = 0
    while(gamestate.is_jousting) {
        play_game_loop(gamestate)
        gamestate.loop += 1
    }

    // allow the AI one last turn
    if (dealer_intelligence(gamestate, true)) show_game_table(gamestate)

    // decide who won - https://www.officialgamerules.org/blackjack
    game_over(gamestate)
}

fn play_game_loop(gamestate) -> {
    gamestate.show_cards = false
    update_gamestate(gamestate)

    // check the game status
    if ((gamestate.dealer_score > 21) || (gamestate.player_score >= 21)) gamestate.is_jousting = false
    else {
        prompt_user(gamestate)
    }

    // allow the dealer to respond && compute the scores
    if (dealer_intelligence(gamestate, false)) gamestate.show_cards = true
    if (gamestate.show_cards) show_game_table(gamestate)
}