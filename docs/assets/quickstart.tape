# docs/assets/quickstart.tape — drt quickstart demo (issue #377)
# VHS-0.11 compatible. Run from the repo root AFTER the one-time setup below:
#
#   rm -rf /tmp/drt-demo && mkdir -p /tmp/drt-demo && cd /tmp/drt-demo
#   drt init --template duckdb_to_rest
#   python -c "import duckdb; c=duckdb.connect('warehouse.duckdb'); c.execute('''CREATE TABLE IF NOT EXISTS users AS SELECT * FROM (VALUES (1,'Alice','alice@example.com'),(2,'Bob','bob@example.com'),(3,'Carol','carol@example.com')) t(id,name,email)'''); c.close()"
#   cd <repo-root> && vhs docs/assets/quickstart.tape

Output docs/assets/quickstart.gif

Set Shell "bash"
Set FontSize 18
Set Width 1200
Set Height 760
Set Padding 22
Set Margin 18
Set MarginFill "#13111c"
Set BorderRadius 10
Set WindowBar Colorful
Set Theme "Catppuccin Mocha"
Set TypingSpeed 42ms

Type "cd /tmp/drt-demo && clear" Enter
Sleep 600ms

Type "drt list" Sleep 400ms Enter
Sleep 2s
Type "drt validate" Sleep 400ms Enter
Sleep 2s
Type "drt run --dry-run" Sleep 400ms Enter
Sleep 2.5s
Type "drt run" Sleep 400ms Enter
Sleep 3s
Type "drt status" Sleep 400ms Enter
Sleep 3s
Sleep 1.5s
