# Jake Flancer
# Get all games in nwhl schedule
library(Rcrawler)


#team ids change every season lol
the1718ids <- 2840761:2840764
the1617ids <- c(2150454, 2150455, 2150457, 2150459)
the1516ids <- 2150711:2150714
#and season id isn't just the year
#1718: 407749
#1617: 327125
#1516: 327151


#Creates urls for each team schedule page
team_urls <- paste("https://www.nwhl.zone/schedule/team_instance/",
                   the1516ids,
                   "?subseason=327151",
                   sep = "")

# Creates empty vector
all_games <- c()

#Iterates through each team
for(i in team_urls){
  #Pulls urls for each game
  Rcrawler(i, urlregexfilter ="/game/show/", MaxDepth=1)
  #Takes out game id
  game_ids <- strsplit(INDEX$Url, split = "\\?|\\/")
  game_ids <- lapply(game_ids, function(x){x[6]})
  game_ids <- unlist(game_ids)
  #adds to vector
  all_games <- c(all_games,game_ids)
}
#since duplicate games
all_games <- unique(all_games)
#MAKE SURE TO CHANGE THIS
write.csv(all_games, file = "/Users/Jake/Dropbox/nwhl/nwhl_gameids1516.csv")

# Rcrawler Creates dumb folders in working directory- this deletes them
deleted <- dir(pattern = "nwhl.zone")
unlink(deleted, recursive = T)