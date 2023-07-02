Output out.gif

Set Shell "zsh"
Set FontSize 32
Set Width 1200
Set Height 600

Type "cargo run companies.db .dbinfo" Enter

Sleep 5s

Type "cargo run companies.db .tables" Enter

Sleep 5s

Type "cargo run companies.db 'select id, name from companies where country = 'eritrea''" Enter

Sleep 5s
