#2. Pobierz dane pogodowe z REST API
#getwd()
install.packages("httr")
install.packages("jsonlite")

library(httr)
library(jsonlite)

#require(httr)  # wyrzuca blad jak nie da sie zaladowac paczki

endpoint <- "https://api.openweathermap.org/data/2.5/weather?q=Warszawa&appid=420755a149bf0448dcbddd3efe540a11&units=metric"
getweather <- GET(endpoint)
weatherText <- content(getweather,"text")
View(weatherText) ## wyswietlamy slownikowo
weatherJSON <- fromJSON(weatherText)
wdf<-as.data.frame(weatherJSON)  ## zmieniamy w ramke danych
View(wdf)

# zmiana jednostek
#&units=metric na koncu endpointa


#1. Wczytaj plik autaSmall.csv i wypisz pierwsze 5 wierszy

df <- read.csv("./autaSmall.csv",fileEncoding = "UTF-8")
View(head(df,5))

#3.Napisz funckję zapisującą porcjami danych plik csv do tabeli  w SQLite

#read.table
#file
#petla
#size - pobieramy ograniczana iloscia wierszy
#instrukcja warunkowa/ przerywanie
#polaczenie z sqlite
#delete - ewentualne usuniecie bazy

readToBase<-function(filepath,dbConn,tablename,size,sep=",",header=TRUE,delete=TRUE)
  {
  ap <- !delete 
  ov <- delete
  fileConnection<-file(description=filepath, open="r")
  fileConnection<-file(description="autaSmall.csv", open="r")
  dd1 <-read.table(fileConnection,nrows=100, header = TRUE, fill = TRUE, sep=",")
  dbWriteTable(con, "autasmall", dd)
  myColNames <- names(dd1) # zapisujemy naglowki


  repeat{
    if(nrow(dd1)==0)
      {
      dbDisconnect(con)
      close(fileConnection)
      break
       }
    dd1 <-read.table(fileConnection,nrows=100, col.names = myColNames, fill = TRUE, sep=",")
    dbWriteTable(con, "autasmall", dd, append = TRUE, overwrite = FALSE)
    }
  }

dbReadTable(con,"autasmall")

#trzeba podmienic size

?read.table

dd <- read.table("./autaSmall.csv",header = TRUE,sep=",",encoding = "UTF-8")
View(dd)
str(dd) # wrzuca sampla

install.packages(c("DBI","RSQLite"))
library(DBI)
library(RSQLite)


# łaczenie sie do bazy 
#DBI::dbwriteTable()
?dbWriteTable   
con <- dbConnect(RSQLite::SQLite(), "db.sqlite")
dbWriteTable(con, "autasmall", dd)
dbReadTable(con, "autasmall")
dbConnection(con)

# wklikujuemy porcjami rekordy do danych
fileConnection<-file(description="autasmall.csv", open="r")

View(read.table(fileConnection,nrows=10, header = TRUE, fill = TRUE, sep=","))

# naglowki z nazwami z dd1
dd1 <-read.table(fileConnection,nrows=10, header = TRUE, fill = TRUE, sep=",")
dbWriteTable(con, "autasmall", dd)
myColNames <- names(dd1) # zapisujemy naglowki
dd2<-View(read.table(fileConnection,nrows=10, col.names = myColNames, fill = TRUE, sep=","))  # ramka z nowymi naglowkami
close(fileConnection)

# funkcja na loop do loadu danych bez spprawdzania kontetnu paczek
#?'repeat'

#i<-1
#repeat{
#  print(i)
#  if(i==5)
#    break
#  i<-i+1}

# sprawdzanie czy ostatni rzad jest zero

fileConnection<-file(description="autasmall.csv", open="r")
dd1 <-read.table(fileConnection,nrows=10, header = TRUE, fill = TRUE, sep=",")
dbWriteTable(con, "autasmall", dd)
myColNames <- names(dd1) # zapisujemy naglowki


repeat{
  if(nrow(dd1)==0){
    dbDisconnect(con)
    close(fileConnection)
    break
  }
  dd1 <-read.table(fileConnection,nrows=10, col.names = myColNames, fill = TRUE, sep=",")
  dbWriteTable(con, "autasmall", dd, append = TRUE, overwrite = FALSE)
}

dd2<-View(read.table(fileConnection,nrows=10, col.names = myColNames, fill = TRUE, sep=","))  # ramka z nowymi naglowkami
close(fileConnection)  


?file

#4.Napisz funkcję znajdującą tydzień obserwacji z największą średnią ceną ofert korzystając z zapytania SQL.
con <-dbConnect(SQLite(),"auta.sqlite")
readToBase("./autaSmall.csv",con,"tabela",size=1000)
res <-dbSendQuery(con, "SELECT* FROM  auta2")
res<- dbSendQuery(con, "SELECT week, max(avg_price) as max_avg_price FROM  (select avg(X19999) as avg_price, x1 as week from tabela) ")
zBazy <- dbFetch(res)


dbClearResult(res)

#5. Podobnie jak w poprzednim zadaniu napisz funkcję znajdującą tydzień obserwacji z największą średnią ceną ofert  tym razem wykorzystując REST api.

fromJSON("http://54.37.136.190:8000/row?id=10000000")