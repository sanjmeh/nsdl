library(assertthat)
library(magrittr)
library(purrr)
library(data.table)

source("nsdlfn.R")


#---- setup variables & options ------
pansm <- "AAOPM4542K"
panrm <- "ABLPM8987G"
sriram <- "1nvbuoZ64jGUnE7vQAJ1aGNQ-CqT4AXJjHJljFDCSNWY"
dir() %>% grep("NSDL",.,va=T) -> files
passwords <- ifelse(grepl("10093",files),pansm,panrm)
options(scipen = 999) # disabling scientific penalty 
options(digits=2)
# create an ordered character array of month_year pattern
expand.grid(month.abb,2015:2018) %>% as.data.table -> .grid1
.grid1[,mstr:=paste(Var1,Var2,sep = "_")]
.grid1[,m:=factor(mstr,levels= mstr,ordered = T)]


#--- Main code----
1:NROW(files) %>% purrr:: map(report) -> list_of_reports
1:NROW(files) %>% purrr::map(function(x) list_of_reports[[x]]$portfolio) %>% rbindlist(fill=T) -> port
1:NROW(files) %>% purrr::map(function(x) list_of_reports[[x]]$mf) %>% rbindlist(fill = T) -> mf
 mf[,title:=str_replace_all(string = str_trim(title),pattern = regex("[ -]+"),replacement = " ")]
 mf[,title:=toupper(str_trim(title))]
 mf$st_date -> dates1
 mf[,mnth:=paste(month.abb[month(dates1)],year(dates1),sep="_")]
 mf[,unit_max:=max(units),by=ISIN]
 mf[,unit_max_tit:=max(units),by=.(title,name)]
 mf$mnth %>% factor(x = .,levels = .grid1$m,ordered = T) ->mf$mnth
 mf[,{mtitle=.SD[order(-nchar(title))][1,title];.(title=mtitle)},by=ISIN] -> maxlookup
 maxlookup[mf,on="ISIN"][,-c("i.title")] -> mf2
 
 mf2[,.(title=first(title),folio=paste(unique(folio),collapse = ";"),
       units=sum(units),curvalue=sum(curvalue),
       NAV=mean(NAV),totcost=sum(totcost),
       unrprofit=sum(unrprofit),annreturn=mean(annreturn)),
    by=.(ISIN,mnth,CAS)] -> mf3
 mf3[str_count(folio,";")>0,.(mnth,ISIN,folio)] -> mfolio
 if(nrow(mfolio)>0) {
   cat("Multiple folio numbers for same ISIN detected:\n")
   print(mfolio)
 }
 mf_curvalue <- mf3 %>% dcast(ISIN + title + CAS ~ mnth,value.var="curvalue",fun.aggregate=sum) %>% .[order(CAS,ISIN)]
 mf_count <- mf3 %>% dcast(CAS ~ mnth,value.var="ISIN",fun.aggregate=function(x) length(unique(x)))
 mf_units <- mf3 %>% dcast(ISIN + title + CAS ~ mnth,value.var="units")
 mf_nav <- mf3 %>% dcast(ISIN + title + CAS ~ mnth,value.var="NAV")
 maxcols <- length(mf_units)
 mf_units[,c(1:3,(maxcols-2):maxcols),with=F] -> mf_units2
 names(mf_units2) <- qc(isin,title,cas,m1,m2,m3)

 mf_units2[,.(title=first(title),count=length(title),totm1=sum(m1,na.rm = T),totm2=sum(m2,na.rm = T),totm3=sum(m3,na.rm = T)),by=isin] -> mf_units3
 
 changes_in_3mths <- mf_units3[! (totm1==totm2 & totm2==totm3)]
  if(nrow(changes_in_3mths)>0) {
    message("Following unit changes in last 3 months:")
    print(changes_in_3mths)
  }
 
 
 port$st_date -> dates2
 port[,mnth:=paste(month.abb[month(dates2)],year(dates2),sep="_")]
 port$mnth %>% factor(x = .,levels = .grid1$m,ordered = T) -> port$mnth
 port_wide <- port %>% dcast(Asset + CAS ~ mnth,value.var="Value",fun.aggregate=sum) %>% .[order(CAS,Asset)]
 
 
 message("NSDL statements read into variables:mf & port\nmf2 has normalised MF names\nmf3 is aggregated on ISIN (folios are conolidated)\nmf_curvalue & port_wide are giving monthwise trend")
 

