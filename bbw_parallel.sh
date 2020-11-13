# GNU parallel for bbw_cli.py. Example for round 3 at SemTab2020.
# --delay:			One second delay between processes.
# --linebuffer:		Output each process.
# --link:			Linking arguments, instead of using all combinations of them.
parallel --delay 1 --linebuffer --link python3 bbw_cli.py --amount ::: 12523 12523 12523 12523 12522 ::: --offset  ::: 0 12523 25046 37569 50092
