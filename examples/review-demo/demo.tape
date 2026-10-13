Output demo.mp4
Output demo.gif

Set Shell "bash"
Set Width 1280
Set Height 720
Set FontSize 22
Set TypingSpeed 45ms
Set PlaybackSpeed 1.0
Set MarginFill "#0b1020"
Set WindowBar Colorful

Type "cp .env.example .env"
Enter
Sleep 2s

Type "./run-demo.sh --reset setup"
Enter
Sleep 8s

Type "./run-demo.sh plan"
Enter
Sleep 10s

Type "./run-demo.sh apply"
Enter
Sleep 10s

Type "./run-demo.sh danger"
Enter
Sleep 16s

Type "# 1,487 deletes refused. The CRM still has all 1,512 rows."
Sleep 5s
