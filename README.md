# OffsideReview Database Stats

Here is the code behind how all the stats for OffsideReview were calculated and inserted into the database. If you are interested in the code for the site itself see [https://github.com/HarryShomer/OffsideReview](here). I was bored so I figured I may as well make this public for those who may be interested. There are four main processes that I'll give a very brief overview on. Before we start I must note/warn that code presented here is, on average, poorly written and incredibly hacky. Also the SQL is incredibly messy as this project was how I learnt the language and I never bothered to refactor. So be careful about what you take away from this. 

## Compile Stats

This was the process used to calculate and insert all the skater, goalie, and team stats into the database. To do this you would run compile\_stats.compile\_stats.compile.process for a given range you want to calculate the stats for. When run properly this process would:

- Scrape the data for all games in that date range.
- Delete any data in that range from the database.
- Find all the new players not in the database and scrape and store their info in the database.
- Rink adjust the pbp 
- Calculate the xG for each shot
- Aggregate the stats for each player and team on a game & strength basis.

This ran on a daily basis. All errors were logged in a log file. 


## Season Projections

This was done to produce the end of season projections displayed on the site for the 2018-2019 season. You would run season\_projections.team\_projections.get\_probs. This would fetch the data for the analysis. Run 'n' simulations of the season (using season\_projections.run_simulations.simulate\_season). This would then return a DataFrame of data that was inserted into the database. This ran on a daily basis. All errors were logged in a log file. 


## Elo Ratings

Elo ratings were a feature in my projection model. It updated a previously stored file containing the scores with the game outcomes from the previous night. This was done by running elo\_ratings.update\_elo.py. This ran on a daily basis. All errors were logged in a log file. 


## Game Predictions

I never actually finished this though it was about ~95% done. 
