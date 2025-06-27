//////////////////////////////////////////////////////////////////////////////////////
//      PLAYING CARDS - BLACKJACK DEMO
// inspired by: https://codereview.stackexchange.com/questions/82103/ascii-fication-of-playing-cards
// include('./demoes/language/BlackJack.oxide')
//////////////////////////////////////////////////////////////////////////////////////

use agg, nsd, str, tools

faces = select face: value from ((2..=10):::map(n -> n:::to_string()) ++ ["J", "Q", "K", "A"]):::to_table()
suits = select suit: value from ["♥️", "♦️", "♣️", "♠️"]:::to_table()

// create the deck, player hand and dealer hand
deck = nsd::save("games.blackjack.deck", faces * suits)
player = nsd::save("games.blackjack.player", faces * suits)
player:::resize(0)
dealer = nsd::save("games.blackjack.dealer", faces * suits)
dealer:::resize(0)

money = 1000.0
bet = 25.0
level = 1

//////////////////////////////////////////////////////////////////////////////////////
//      UTILITY METHODS
//////////////////////////////////////////////////////////////////////////////////////

fn computeScore(aceScore: i64) -> {
    (select
        score: sum(match face {
            "A" => aceScore
            face when face in "2".."9" => i64(face)
            _ => 10
        })
    from hand)[0][0]
}

fn getCardScore(hand) -> {
    v11 = computeScore(11)
    if(v11 <= 21, v11, computeScore(1))
}

fn dealerScore() -> getCardScore(dealer)

fn hit(hand) -> hand <~ deck

fn playerScore() -> getCardScore(player)

fn dealerIntelligence(finish: Boolean = false) -> {
    let modified = false
    _playerScore = playerScore()
    if((_playerScore <= 21) && (dealerScore() < _playerScore)) {
        let cost =
            if (finish) while (_playerScore > dealerScore()) hit(dealer)
            else if(_playerScore > dealerScore()) hit(dealer)
        modified = modified || (cost.inserted > 0)
    }
    modified
}

fn faceUp(face, suit) -> {
    faceL = if(face:::len() < 2, face + " ", face)
    faceR = if(face:::len() < 2, " " + face, face);
    [
        '┌─────────┐',
        '│ {{faceL}}    {{suit}} │',
        '│         │',
        '│    {{suit}}    │',
        '│         │',
        '│ {{suit}}    {{faceR}} │',
        '└─────────┘'
    ]
}

fn faceDown() -> [
    "┌─────────┐",
    "│░░░░░░░░░│",
    "│░░░░░░░░░│",
    "│░░░░░░░░░│",
    "│░░░░░░░░░│",
    "│░░░░░░░░░│",
    "└─────────┘"
]

fn showTitle() -> {
  println(
        "|        _             _                                 _
         |  _ __ | | __ _ _   _(_)_ __   __ _    ___ __ _ _ __ __| |___
         | | '_ \| |/ _` | | | | | '_ \ / _` |  / __/ _` | '__/ _` / __|
         | | |_) | | (_| | |_| | | | | | (_| | | (_| (_| | | | (_| \__ \
         | | .__/|_|\__,_|\__, |_|_| |_|\__, |  \___\__,_|_|  \__,_|___/
         | |_|            |___/         |___/
         |":::strip_margin('|'))
}

//////////////////////////////////////////////////////////////////////////////////////
//      MAIN PROGRAM
//////////////////////////////////////////////////////////////////////////////////////

showTitle()

let isAlive = true
while(isAlive) {
    // reset and shuffle the deck
    nsd::save("demo.game.blackjack", faces * suits)
    deck:::shuffle()

    // put some cards into the hands of the player and dealer
    for hand in [dealer, player] { 
        hand:::resize(0)
        hit(hand) 
    }

    isJousting = true
    betFactor = 1.0

    fn showSeparator() -> {
        separator = ("※" * 120) + '\n'
        println('\n' + separator)
        println(' Player: {{__userName__}} \t Credit: ${{money}} \t Bet: ${{bet}} \t Round: {{level}} \n')
        println(separator + '\n')
    }

    fn showHand(cards) -> {
        let lines = []
        for card in cards {
            isVisible = true
            let _card = if(isVisible is true, faceUp(face, suit), faceDown())
            lines = if(lines:::len() == 0, _card, (lines <|> _card):::map(a -> a:::join(" ")))
        }

        // add a face down card in the last position
        if (cards == dealer) {
            _card = faceDown()
            lines = if(lines:::len() == 0, _card, (lines <|> _card):::map(a -> a:::join(" ")))
        }

        // write to STDOUT
        println((lines:::join('\n') + '\n'))
    }

    fn showGameTable() -> {
        flag = if(betFactor == 2.0, "2x ", "")
        println('DEALER - {{dealerScore()}}/21')
        showHand(dealer)
        println('YOU ({{__userName__}}) - {{flag}}{{playerScore()}}/21')
        showHand(player)
    }

    // display the hands of the player and dealer
    showSeparator(); showGameTable()

    // main loop
    loop = 0
    while(isJousting) {
        showCards = false

        // check the game status
        if ((dealerScore() > 21) || (playerScore() >= 21)) isJousting = false
        else {
            println('Choose {{ if(loop == 0, "[D]ouble-down, ", "") }}[H]it, [S]tand or [Q]uit? ')
            choice = io::stdin():::trim():::toUpperCase()
            if ((choice:::starts_with("D") is true) && (loop == 0)) betFactor = 2.0
            else if(choice:::starts_with("H")) { hit(player); showCards = true }
            else if(choice:::starts_with("Q")) { isJousting = false; isAlive = false }
            else if(choice:::starts_with("S")) isJousting = false
        }

        // allow the dealer to respond && compute the scores
        if (dealerIntelligence()) showCards = true
        if (showCards) showGameTable()
        loop += 1
    }

    fn roundCompleted(message: String, betDelta: Double) -> {
        printf('%s\n', message)
        if (is_defined(bet)) money += betFactor * betDelta
    }

    // allow the AI one last turn
    if (dealerIntelligence(true)) showGameTable()

    // decide who won - https://www.officialgamerules.org/blackjack
    if (dealerScore() == playerScore()) roundCompleted('🤝Draw.', 0)
    else if (playerScore() == 21) roundCompleted('👍You Win!! - Player BlackJack!', 1.5 * bet)
    else if (dealerScore() == 21) roundCompleted('👎You Lose - Dealer BlackJack!', -bet)
    else if (dealerScore() > 21) roundCompleted('👍You Win!! - Dealer Busts: {{dealerScore()}}', bet)
    else if (playerScore() > 21) roundCompleted('👎You Lose - Player Busts: {{playerScore()}}', -bet)
    else if (playerScore() > dealerScore()) roundCompleted('👍You Win!! - {{playerScore()}} vs. {{dealerScore()}}', -bet)
    else roundCompleted('👎You Lose - {{dealerScore()}} vs. {{playerScore()}}', -bet)
    level += 1
}

money