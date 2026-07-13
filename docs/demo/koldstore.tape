Output docs/assets/koldstore-demo.gif

Require docker

Set Shell bash
Set Width 1250
Set Height 750
Set FontSize 15
Set TypingSpeed 5ms
Set Theme "Catppuccin Frappe"
Set WindowBar Colorful
Set BorderRadius 7
Set Margin 0
Set MarginFill "#1e1e2e"
Set CursorBlink false

# Start and prepare PostgreSQL without recording it.
Hide

Type "docker rm -f koldstore-readme-demo >/dev/null 2>&1 || true"
Enter

Type "docker run -d --platform linux/amd64 --name koldstore-readme-demo -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=koldstore -p 55432:5432 jamals86/pg-koldstore:latest"
Enter

Type "until docker exec koldstore-readme-demo pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done"
Enter
Sleep 2s

# 1. Instantly speed up the frame rate for the 45-second script
Set PlaybackSpeed 3.5

Type "docker exec -i koldstore-readme-demo psql -U postgres -d koldstore < docs/demo/setup.sql"
Enter

Type "docker cp docs/demo/koldstore-demo.sql koldstore-readme-demo:/tmp/koldstore-demo.sql"
Enter

Type "clear"
Enter

Show

Set TypingSpeed 20ms
Type "docker exec -it -e PAGER=cat koldstore-readme-demo psql -X -q -e -v ON_ERROR_STOP=1 -U postgres -d koldstore -f /tmp/koldstore-demo.sql"
Enter

# The SQL script controls pacing with hidden \! sleep commands (~45s runtime).
Sleep 45s

Hide

Type "docker rm -f koldstore-readme-demo >/dev/null"
Enter
