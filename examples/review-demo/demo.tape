Output demo.mp4
Output demo.gif

Set Shell "bash"
Set Width 1280
Set Height 720
Set FontSize 19
Set TypingSpeed 45ms
Set PlaybackSpeed 1.0
Set MarginFill "#0b1020"
Set WindowBar Colorful

Type "cp .env.example .env"
Enter
Wait /> ?$/
Sleep 1s

Type "./run-demo.sh --reset setup"
Enter
Wait+Screen@5m /Ready: DuckDB warehouse/
Sleep 2s

Ctrl+L
Sleep 500ms
Type "./run-demo.sh plan"
Enter
Wait+Screen@2m /Review checkpoint: 450 updates/
Sleep 2s

Ctrl+L
Sleep 500ms
Type "./run-demo.sh apply"
Enter
Wait+Screen@2m /Verified: 450 blanks filled/
Sleep 1s
Type "# All 1,500 rows go through fill_empty; only the 450 blanks change."
Sleep 3s
Enter
Sleep 1s

Ctrl+L
Sleep 500ms
Type "./run-demo.sh danger"
Enter
Wait+Screen@2m /drt apply exit code: 1/
Wait+Screen@2m /Verified: 450 blanks filled/
Sleep 2s

Type "# 1,487 deletes refused. The CRM still has all 1,512 rows."
Sleep 5s
