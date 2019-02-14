library(aplpack)

setwd("C:/Charles/Projects/data-viz/R/20190210 - Reddit, Drug Harm and Dependence")
drugs <- read.csv("drugs.csv")
drugs[1,]

# re-order columns
drugs <- drugs[c("Drug", "Mean.Social.harm", "Health.care.costs", "Intoxication", "Pleasure", "Pleasure", "Pleasure", "Psychological", "Psychological", "Acute.harm", "Chronic.harm", "Intravenous.harm", "Physical", "Physical", "Physical", "Physical")]
drugs[1,]

faces(
  drugs[2:16],
  # face.type=1,
  labels=drugs$Drug,
  main='Chernoff Faces of Drug Harm and Dependence',
  scale=TRUE,
  cex=1.5,
  ncolors=3
  # col.hair=colors,
)
