# Statements

Sinex is a personal information management system. It absorbs all personally relevant data, broadly construed. It builds increasingly abstract data points derived from multitude of lower level ones. It enables user to query across the entire dataset, correlating data points in ways both simple or intricate. It takes advantage of LLMs. 

Some of the most basic kinds of data the system absorbs include: low-level user input, screen recordings, continuous audio input capture. One kind of derived data is screen OCR, derived from screen recordings. Another is ASR, derived from captured audio input.

On a slightly higher level, we capture window manager data regarding which window is focused, and content of all terminals (through asciinema setup to always run). The latter makes screen recordings redundant, but that's considered okay. If needed, the system has enough data to remove such redundant data precisely. Similarly, we capture user input through a compositor (Hyprland) plugin, even through we also capture input at a lower level. It is not exactly the same data (otherwise there really wouldn't be a point capturing both kinds). 

