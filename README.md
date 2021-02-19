# Rev Project 2 - Emoji Twitter Data - Team ACCE

## Project Description

This purpose of this project is to use Twitter data to analyze the use of emojis in several contexts:
1) Most popular emoji
2) Most used emoji for a given day
3) Emoji count / Number of Words
4) Most mentioned user with a particular emoji
5) Emoji usage in different categories
6) Emoji usage in different locations
7) Emoji usage in the past vs today

## Technologies Used

* Apache Spark
* Scala
* Scala Metals
* Twitter Developer APIs
* SBT
* VSCode
* Hydrator - (Takes 'dehydrated' tweet ids from old tweets and uses it to reproduce the original tweet with all its data preserved.)

## Getting Started

To download the repo:
```git clone https://github.com/HomelessSkittle/RevProject2-TeamACCE.git```

***IMPORTANT - In the directory for each question, be sure to create a "twitterstream" folder, or else the spark-submission will fail.***

## Usage

Navigate to the particular question directory you are intersted in,
Import the build using Scala Metals then run
```sbt assembly```
then
```spark-submit target/scala-2.11/question (and hit tab to complete the command)```

## Contributors

> Adam Pesch
> Christian Palko
> Conor Sosh
> Eunice Lee

## License

This project uses the following license: [MIT License](<https://mit-license.org/>).
