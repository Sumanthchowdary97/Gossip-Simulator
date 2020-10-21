COP5615 - Fall 2020
Project 2 - Gossip Simulator

Members: Sai Chandra Sekhar Devarakonda (UFID: 9092-2981), Sumanth Chowdary Lavu (UFID: 5529-6647)

Execution:
-->dotnet fsi --langversion:preview script.fsx nodes topology algorithm
For Example: dotnet fsi --langversion:preview script.fsx 100 line gossip

Working: a) Gossip algorithm converges for Line, Full Network, 2D Grid, Imperfect 2D Grid topologies.
	 b) Push-Sum algorithm converges for Line, Full Network, 2D Grid, Imperfect 2d Grid topologies.

Largest Network handled:

-->With Gossip algorithm for all the topologies the largest network handled had 3000 nodes.
-->With Pushsum algorithm for all the topologies the largest network handled had 2000 nodes.
