# these are functions to read NSDL pdf files into processed data
library(magrittr)
library(lubridate)
library(wrapr)
library(pdftools)
library(readtext)
library(textclean)
library(data.table)
library(splitstackshape)
library(stringr)
library(htmlTable)
library(udpipe)
library(googlesheets)
library(ggplot2)
#library(ggrepel)
library(readxl)

frto <- function(string=NULL,range=NULL){
  string[range[1]:range[2]]
}

#show lines matching a pattern in a given file
showl <- function(file=1,pass="sm",pat="Demat",nlines=2){
  finfo <- pdf_info(pdf = files[file],upw = ifelse(pass=="sm", pansm,panrm))
  ftxt <- pdf_text(pdf = files[file],upw = ifelse(pass=="sm", pansm,panrm))
  for (p in 1:finfo$pages){
    ftxt[[p]] %>% str_split(pattern = "\n") %>% unlist %>% str_trim -> pagetext
    line<- grep(pat,pagetext,ig=T)
    if( length(line)>0 ){
      mat_ranges <- matrix(c(line,line+nlines),ncol=2)
      mat1<- apply(X = mat_ranges,FUN = frto,MARGIN = 1,string=pagetext)
      message(paste("Page:",p,":","lines:",paste(mat_ranges,collapse = ":")))
      print(mat1)
    }
  }
}

pullout <- function(fulltext=NULL,rangedt=NULL){
  pagestart <- rangedt[1,2]$pgno
  pageend <- rangedt[2,2]$pgno
  linestart <- rangedt[1,3]$lineno
  linetotal <- rangedt[2,3]$lineno
  dump<- character(0)
  for (p in pagestart:pageend){
    fulltext[[p]] %>% str_split(pattern = "\n") %>% unlist %>% str_trim -> pagetext
    lastline <- NROW(pagetext)
    if (p==pagestart) startline <- linestart
    if (p==pageend) lastline <- linetotal
    if (p>pagestart & p <= pageend)  linestart <- 1
    c(dump,pagetext[linestart:lastline] ) -> dump
  }
  dump
}

#  extract porfolio summary into a DT
dtport <- function(fulltext,dtr){
  pullout(fulltext = fulltext,rangedt = dtr) -> extr
  dtraw <- data.table(extr=extr)
  dtraw2 <- cSplit(dtraw,"extr",sep = "  ",type.convert = "as.character")
  dtraw3 <- dtraw2[,lapply(.SD,str_remove_all,",")]
  dtraw4 <- dtraw3[,lapply(.SD,str_remove_all,"%")]
  dtraw4 <- dtraw4[,lapply(.SD,str_trim)]
  
  names(dtraw4) <- qc(Asset,Value,Percent)
  dtraw4 <- dtraw4[! grepl("ASSET|`",Asset,ignore=T)]
  dtraw4 <- dtraw4[grepl("Total",Asset,ignore=T),Asset:="TOTAL"]
  
  for (cols in 2:3) set(dtraw4, j = cols, value = as.numeric(dtraw4[[cols]]))
  dtraw4[,Asset:=toupper(Asset)]
  dtraw4[,st_date:=findp(fulltext)[1]][,end_date:=findp(fulltext)[2]][,name:=getid(fulltext)[1]][,CAS:=getid(fulltext)[2]][,report:="PORTFOLIO"]
  dtraw4
}
dtmf <- function(fulltext,dtr) {
  # internal function to filter out the unnatural words picked up in fund name
  fn <- function(var) ifelse(grepl("^[0-9]{5,}|^[0-9]{1,}\\.[0-9]{1,}|NOT|AVAILABLE|SBBN|Total|ISIN|UCC",var) | is.na(var), "", str_trim(var))
  
  #---- pull raw text out from line number range in dtrMF ----
  pullout(fulltext = fulltext,rangedt = dtr) -> extr
  dtraw <- data.table(extr=extr)
  dtraw2 <- cSplit(dtraw,"extr",sep = " ",type.convert = "as.character")
  if(names(dtraw2)[1]=="extr_1") names(dtraw2) %<>% str_replace(pattern = "_([0-9])","_0\\1")
  dtraw3 <- dtraw2[,lapply(.SD,str_remove_all,",")]
  dtraw3 <- dtraw3[!(extr_01=="Page" & grepl("\\d+$",extr_02))]
  dtraw3 <- dtraw3[!(extr_01=="Summary" & grepl("Holdings",extr_02))]
  dtraw3 <- dtraw3[!extr_01=="ISIN"]
  dtraw3 <- dtraw3[!(extr_01=="UCC" & extr_02=="Units")]
  dtraw3 <- dtraw3[!(extr_01=="Per" & extr_02=="Units")]
  dtraw3 <- dtraw3[!(extr_01=="`" & is.na(extr_02))]
  
  
  #---- extract header of the MF table -----
  pg_start <- dtr[instance=="startMFF",pgno]
  line_mfheader <- dtr[instance=="startMFF",lineno+1]
  mfheader <- fulltext[[pg_start]] %>% str_split(pattern = "\n") %>% .[[1]] %>% .[line_mfheader] %>% str_split(.,pattern = "\\s{2,}") %>% .[[1]]
  mfheader2 <- fulltext[[pg_start]] %>% str_split(pattern = "\n") %>% .[[1]] %>% .[line_mfheader+1] %>% str_split(.,pattern = "\\s{1,}") %>% .[[1]]
  if(length(mfheader)<8) mfheader <- qc(ISIN,title,folio,units,NAV,curvalue) else mfheader <- qc(ISIN,title,folio,units,cpunit,totcost,NAV,curvalue,unrprofit)
  #----- identify the dangling digits (upto 2 digits) of folio number ----
  str_detect(as.matrix(dtraw3),"^[0-9]{1,2}$") %>% matrix(nrow=nrow(dtraw3)) %>% .[,1:8] -> mata  # lgl matrix of dangling numbers 
  str_detect(as.matrix(dtraw3),regex("series",ig=T)) %>% matrix(nrow=nrow(dtraw3)) %>% .[,1:8] -> matb  # lgl matrix of rows with SERIES (because we need to keep the dangling number after the string SERIES)
  is.na(as.matrix(dtraw3)) -> matc 
  mata & !is.na(mata) -> mata 
  matb & !is.na(matb) -> matb
  which(max.col(mata,ties.method = "first")>1) -> ind1 # index of row numbers if the pure number is NOT the first column: likely columns are 2,3 or 4.
  grep("^INF",dtraw3$extr_01)+1 -> ind2 #  index of row numbers immediately next to ^INF - dangling digits can only be in next rows.
  which(mata,arr.ind = T) %>%   data.table -> dt_a
  which(matb,arr.ind = T) %>%   data.table -> dt_b
  which(matc,arr.ind = T) %>%   data.table -> dt_c
  dt_ignore <- dt_b[dt_a,on="row"] # the rows that have SERIES (matb) are to be ignored
  rows_to_ignore <- ifelse(dt_ignore$col+1==dt_ignore$i.col,dt_ignore$row,NULL) # ignore the dangling nunber that falls in next column to the one having SERIES
  intersect(ind1,ind2) -> ind3 #   rows that have dangling digits - common rows in ind1 & ind2
  ind3 <- setdiff(ind3,rows_to_ignore) # If SERIES and a next column has a 2 digit number as it is likely to be part of a fund title
  if(length(ind3)>0){
    dtraw3[ind3,dig3:=str_extract(string = extr_03,pattern = "^[0-9]{1,2}$")]
    dtraw3[ind3,dig4:=str_extract(string = extr_04,pattern = "^[0-9]{1,2}$")]
    dtraw3[ind3,dig5:=str_extract(string = extr_05,pattern = "^[0-9]{1,2}$")]
    dtraw3[ind3,dig6:=str_extract(string = extr_06,pattern = "^[0-9]{1,2}$")]
    dtraw3[,dig_append:=ifelse(!is.na(dig4),dig4, ifelse(!is.na(dig5),dig5,ifelse(!is.na(dig6),dig6,"")))]
    dtraw3[,dig_append:=shift(dig_append,n = 1,type = "lead")]
    dtraw3[ind3,extr_04:=str_remove(string = extr_04,pattern = "^[0-9]{1,2}$")]
    dtraw3[ind3,extr_05:=str_remove(string = extr_05,pattern = "^[0-9]{1,2}$")]
  }
  
  # collect the fund name from various wrapping positions (avoiding the first column for the first two rows) ----
  dtraw3[,title:=str_c(fn(extr_02),
                       fn(extr_03),
                       fn(extr_04),
                       fn(extr_05),
                       fn(txt_next(extr_02)),
                       fn(txt_next(extr_03)),
                       fn(txt_next(extr_04)),
                       fn(txt_next(extr_05)),
                       fn(txt_next(extr_01,2)),
                       fn(txt_next(extr_02,2)),
                       fn(txt_next(extr_03,2)),
                       fn(txt_next(extr_04,2)),
                       fn(txt_next(extr_05,2)),
                       fn(txt_next(extr_01,3)),
                       fn(txt_next(extr_02,3)),
                       fn(txt_next(extr_03,3)),
                       fn(txt_next(extr_04,3)),
                       fn(txt_next(extr_05,3)),
                       fn(txt_next(extr_01,4)),
                       fn(txt_next(extr_02,4)),
                       sep = " ")]
  dtraw3[grepl("INF",extr_01)] -> dtraw4 
  # generate boolean matrix with min 6 consecutive digits or SBBN (for Sundaram non stand folio) for possible folio numbers ----
  str_detect(as.matrix(dtraw4),"^[0-9]{6,}|SBBN[A-Z0-9]{4,}") %>% matrix(nrow=nrow(dtraw4)) %>% .[,2:10] -> matb 
  matb[which(is.na(matb))] <- F
  max.col(m = matb,ties.method = "first") -> seqm
  dtraw4[,col_seq:=seqm+1]
  dtraw4[, folio := .SD[[col_seq]], 1:nrow(dtraw4)]
  dtraw4$title %>% str_locate(.,"INF") %>% .[,1] -> x1 # to detect location of next INF so we can cut the title till the exact end
  dtraw4$title %>% str_sub(start = 1, end = ifelse(is.na(x1),-1,x1-1)) %>% str_trim() -> dtraw4$title
  if(length(ind3)>0) dtraw4[,folio:=paste0(folio,dig_append)]
  if(length(mfheader)>6) dtraw5 <- 
    dtraw4[,{
      .(title,folio,
        ISIN=extr_01,
        units=get(colnames(.SD)[col_seq+1]),
        cpunit=get(colnames(.SD)[col_seq+2]),
        totcost=get(colnames(.SD)[col_seq+3]),
        NAV=get(colnames(.SD)[col_seq+4]),
        curvalue=get(colnames(.SD)[col_seq+5]),
        unrprofit=get(colnames(.SD)[col_seq+6]),
        annreturn=get(colnames(.SD)[col_seq+7])
      )},
      by=1:nrow(dtraw4)] else 
        dtraw5 <- 
    dtraw4[,{
      .(title,folio,
        ISIN=extr_01,
        units=get(colnames(.SD)[col_seq+1]),
        NAV=get(colnames(.SD)[col_seq+2]),
        curvalue=get(colnames(.SD)[col_seq+3]),
        totcost=NA,
        unrprofit=NA,
        annreturn=NA
      )},
      by=1:nrow(dtraw4)]  # this is tricky and should be simplified
  
  
  suppressWarnings(dtraw5[,lapply(.SD,as.numeric),.SDcols=c(5:length(dtraw5))]) -> dt_numcols
  dtraw6 <- cbind(dtraw5[,c(2:4)],dt_numcols)
  dtraw6[,st_date:=findp(fulltext = fulltext)[1]][,end_date:=findp(fulltext)[2]][,name:=getid(fulltext)[1]][,CAS:=getid(fulltext)[2]][,report:="MF"]
} # this is the largest function to extract MFs from the raw text

# dteq is a new function for extracting DEMAT data rewritten with logic not in dtport and dtmf, eventually we will move the others also to this logic
dteq <- function(flattext,bank,clientid,dpid,name,stline,endline){
  dtraw <- data.table(extr = flattext[stline:endline])
  dtraw2 <- cSplit(dtraw,"extr",sep = "  ",type.convert = "as.character")
  dtraw3 <- dtraw2[,lapply(.SD,str_remove_all,",")]
  names(dtraw3) <- qc(stock,co,face,shares,price,value)
  dtraw3[str_detect(price,"\\*")] -> highest_trded
  suppressWarnings(dtraw3$face %<>% as.double())
  suppressWarnings(dtraw3$shares %<>% as.integer())
  
  if(nrow(highest_trded) >0) {
    message("ALERT: HIGHEST TRADED PRICE:",appendLF = F)
    cat(paste0("Report ending:",findp(flattext,flat = T)[2],"; "))
    cat(paste0("Number of stocks:",nrow(highest_trded),"; "))
    cat(paste("Bank:",bank,"\n"))
  }
  suppressWarnings(dtraw3[,price:=str_replace(price,"\\*","") %>% as.numeric])
  #if(findp(fulltext = flattext,flat = T)[2]==ymd("20180131")) browser()
  suppressWarnings(dtraw3$price %<>% as.double())
  suppressWarnings(dtraw3$value %<>% as.double())
  x1 <- dtraw3[!grepl("Differential|Shares|Stock|ISIN",stock)]
  
  x1[,newcol := shift(face)]
  x1 <- x1[! (is.na(face) & is.na(newcol))]
  x1[,rn := as.numeric(rownames(x1))]
  x1[,code := shift(stock,type = "lead")]
  x1[,co2 := shift(co,type = "lead")]
  x1[,co3 := str_trim(paste(co,ifelse(is.na(co2),"",co2)))]
  dtraw4 <- x1[rn %% 2 != 0,.(ISIN=stock,code,company=co3,face,shares,price,value)]
  
  #for (cols in 2:3) set(dtraw4, j = cols, value = as.numeric(dtraw4[[cols]]))
  dtraw4[,report:="DEMAT"][
    ,bank:=bank][
      ,client:=clientid][
        ,dpid:=dpid][
          ,name:=name][
            ,st_date := findp(flattext,flat = T)[1]][
              ,end_date := findp(flattext,flat = T)[2] ]
  dtraw4[,mnth:=crfact(end_date)]
}
  
getid <- function(fulltext=NULL){
  fulltext[[2]] %>% str_split(pattern = "\n") %>% unlist -> pagedump
  grep("CAS ID:",pagedump) -> index
  header <- pagedump[index:(index+1)] %>% str_trim
  name <- header %>% str_split_fixed(pattern = ":",2) %>% str_trim() %>% .[2]
  casid <- header %>% str_split_fixed(pattern = ":",2) %>% str_trim() %>% .[3]
  
  fulltext[[length(fulltext)]] %>% str_split(pattern = "\n") %>% unlist -> pagedump
  grep("Digitally signed by DS NATIONAL SECURITIES",pagedump) -> index
  sign_date <- pagedump[index+2] %>% str_trim
  
  return(c(name=name,casid=casid,signedon=sign_date))
}

# read file number n and dump the text without headers
rmheaders <- function(n,pass){
  dump <- pdf_text(files[n],upw=pass) %>% str_split("\n") %>% unlist %>% str_trim %>% drop_element_fixed("")
  h1 <- dump %>% str_replace_all("\\s+","") %>% grep("page\\d",., ig=T)
  h2 <- dump %>% str_replace_all("\\s+","") %>% grep("summaryholdingstran",., ig=T)
  seq_along(dump) %in% c(h1,h2) %>% not %>% dump[.]
}
# the next 4 functions are used inside the report function
findp <- function(fulltext=NULL,flat=F){ # find period of a statement
  if(flat)  fulltext %>% grep("Statement for the period from",.,value = T) -> tmp else
    fulltext[[2]] %>% str_split(pattern = "\n") %>% unlist %>% str_trim %>% grep("Statement for the period from",.,value = T) -> tmp
  st_date<- str_replace(tmp,"Statement for the period from (\\d{2}.{9}).+$","\\1") %>% dmy
  end_date<- str_replace(tmp,"Statement for the period from.+(\\d{2}.{9})$","\\1") %>% dmy
  per<- c(st_date,end_date)
} # returns period of the statement as two dates
getlinesMF <- function(ftxt=NULL,finfo=NULL,period=NULL){
  stlineMFF=NULL;stpageMFF=NULL;
  tot_pages=finfo$pages; fulltext=ftxt
  p=1;confirmed_line<- NA_integer_
  while(p <= tot_pages & is.na(confirmed_line)){
    fulltext[[p]] %>% str_split(pattern = "\n") %>% unlist -> pagetext
    flineMFF=which(str_detect(str_remove_all(pagetext," "),fixed("MutualFundfolios(f)",ignore=T)))
    #flineERCP=which(str_detect(str_remove_all(pagetext," "),fixed("TotalExpenseRatio&CommissionPaid",ignore=T)))
    if(length(flineMFF)>0){
      indx <- which(str_detect(str_trim(pagetext),regex("^ISIN")))
      confirmed_line <- flineMFF[which((flineMFF+1) %in% indx)[1]]
      if(!is.na(confirmed_line)) { stlineMFF=confirmed_line; stpageMFF=p }
    }
    p <- p+1
  }
  #if(p==tot_pages & is.null(confirmed_line)) message(paste("Could'nt find Mutual Fund token in file signed on:",signedon))
  
  if(length(stlineMFF)>0){
    dt <- data.table(instance="startMFF", pgno=stpageMFF,lineno=stlineMFF)
    
    for (p in stpageMFF:tot_pages){
      fulltext[[p]] %>% str_split(pattern = "\n") %>% unlist -> pagetext
      totline=which(str_detect(str_trim(pagetext),"^Total\\s+\\d+"))
      if(length(totline) >0) {
        if( p==stpageMFF & any(totline>stlineMFF) )
          endlineMFF <- totline[min(which(totline>stlineMFF))]
        else if(p>stpageMFF) endlineMFF <- totline[1]
        
        if(exists("endlineMFF")) dt <- rbind(dt,data.table(instance="endMFF", pgno=p,lineno=endlineMFF))
      }
    }
    return(dt)
  } else
    return("NO LINE FOUND")
}
getlinesPort <- function(ftxt=NULL,finfo=NULL,period=NULL){
  beginline=NULL;page=NULL;endline=NULL
  tot_pages=finfo$pages; fulltext=ftxt
  p=1;confirmed_line<- NA_integer_
  while(p <= tot_pages & is.na(confirmed_line)){
    fulltext[[p]] %>% str_split(pattern = "\n") %>% unlist -> pagetext
    fline=which(str_detect(str_remove_all(pagetext," "),fixed("portfoliocomposition",ignore=T)))
    if(length(fline)==0) fline=which(str_detect(str_remove_all(pagetext," "),fixed("summaryofvalueofholdings",ignore=T)))
    flinetot=which(str_detect(str_remove_all(pagetext," "),fixed("Total",ignore=T)))
    if(length(fline)>0){
      regx1 <- regex("^ASSET CLASS",ignore=T)
      head_row <- str_detect(str_trim(pagetext),regx1)
      indx <- which(head_row)
      gap_of_3lines <- (indx-fline) %in% 1:3
      if(any(gap_of_3lines)) confirmed_line <- indx[gap_of_3lines]
      endline <- flinetot[(flinetot - fline) %in% 11:15]
      if(!is.na(confirmed_line)) { beginline=confirmed_line; page=p }
    }
    p <- p+1
  }
  
  if(length(beginline)>0){
    dt <- data.table(instance="startPORT", pgno=page,lineno=beginline)
    assert_that(exists("dt"),any(dt$instance=="startPORT"))
    dt <- rbind(dt,data.table(instance="endPORT", pgno=page,lineno=endline))
  }
  if(p==tot_pages & is.null(confirmed_line)) return("NO LINE FOUND") else
    return(dt)
}

NSDL <- function(n){
  #if(!exists("files")) dir() %>% grep("NSDL",.,va=T) -> files
  if(!exists("passwords")) passwords <- ifelse(grepl("10093",files),pansm,panrm)
  dump <- rmheaders(n,passwords[n])
  filetext <- dump %>% str_replace_all("\\s+","") 
  nlines <- filetext %>% grep("^NSDLDematAccount$",., ig=T)
  alines <- filetext %>%  grep("ACCOUNTHOLDER",.,ig=T)
  correctnlines <-  intersect(alines-1,nlines)
  if(length(correctnlines)==0) return(data.table())
  assert_that(filetext[correctnlines+3] %>% unique %>% length ==1) 
  assert_that(filetext[correctnlines+3] %>% unique %>% length ==1) 
  banknames <- filetext[correctnlines+2] %>% unique
  dpid <- filetext[correctnlines+4] %>% str_sub(6,13)
  name <- filetext[correctnlines+3] %>% str_extract("[A-Z]+")
  clientid <- filetext[correctnlines+4] %>% str_sub(23,30)
  titline <- filetext %>% grep("^stocksymbol",., ig=T)
  subtotline <- filetext %>% grep("^subtotal",., ig=T)
  titline <- map_int(seq_along(correctnlines),~titline[titline > correctnlines[.]][1]) # first row immediately greater than NSDL title
  subtotline <- map_int(seq_along(titline),~subtotline[subtotline > titline[.]][1]) # first row immediately greater than Stock title
  stline <- titline +1
  endline <- subtotline -1
  seq_along(banknames) %>% map_dfr(~dteq(dump,banknames[.],clientid[.],dpid[.],name[.],stline[.],endline[.]))
}

# main program to read NSDL
report<- function(filen=1){
  pass <- passwords[filen] # an array the length of files containing passwords for each file
  txt <- pdf_text(pdf = files[filen],upw = pass)
  info <- pdf_info(pdf= files[filen],upw = pass)
  message(paste("Starting processing file no.:", filen,"name:",files[filen]))
  per<- findp(fulltext = txt)
  
  getlinesMF(ftxt = txt,finfo = info,period=per) -> dtrMF
  getlinesPort(ftxt = txt,finfo = info,period=per) -> dtrPort
  #getlinesEq(ftxt = txt,finfo = info,period=per) -> dtrEq
  assert_that(is.data.table(dtrMF),is.data.table(dtrPort))
  
  mf_extract <- dtmf(fulltext = txt, dtr = dtrMF)
  print(paste("file",files[filen],"read in with",nrow(mf_extract),"rows"))
  list(mf=mf_extract,portfolio=dtport(fulltext = txt, dtr= dtrPort),equity=NSDL(filen))
}

# plots are two types: NSDL MF fund wise / type wise / manager wise or Total portfolio : equity, mf or total (default)
plot.nsdl <- function(mgr="SRERAM",class="DEBT",name="SM",portfolio=F,start=20180101,fund="total"){
  # leave class="" if you want all classes of funds
  # leave mgr="" if you want all fund managers combined
  cas <- ifelse(name=="SM",100934978,101651322)
    data <- mf2[CAS==cas & grepl(mgr,manager,ig=T) & grepl(class,type,ig=T) & st_date>=ymd(start)]
       basicplot <- ggplot(data,aes(mnth,curvalue,fill=title)) + geom_col() 
       
       if(!portfolio & nchar(mgr)>0) 
         basicplot + facet_wrap(~title,scales = "free_y")
       else if(!portfolio & nchar(class)>0) 
         basicplot + facet_wrap(~manager,scales = "free_y")
       else {
         main_title <- switch(fund,total="TOTAL PORFOLIO",mf="MUTUAL FUND PORTFOLIO",equity="EQUITY PORTFOLIO")
         subtitle <- switch(name,SM="SANJAY MEHROTRA",RM="RASHMI MEHROTRA")
         assetstr <- switch(fund,total="TOTAL",mf="MUTUAL FUND FOLIOS",equity="EQUITIES")
         data <- port[CAS==cas & grepl(assetstr,Asset) & st_date>=ymd(start)]
         ggplot(data,aes(mnth,Value)) +
           geom_point(size=3) + geom_line(group="Asset") + 
           ggtitle(label = main_title,subtitle = subtitle)
       }
}

# charts a facet chart for all stocks  : will take some time to plot
plot.eq <- function(equitydt = eqlive, variable="value", axis = "free"){
  if(variable=="price") var2 <- "profit" else var2 <- "profit2"
  equitydt[,.(get(variable),end_date),by=.(bank,code)] %>% 
    ggplot(aes(end_date,V1,group=interaction(bank,code))) + 
    geom_line(aes(color=code)) + geom_point(aes(color=code)) + 
    facet_wrap( ~ code,scales=axis) + 
    ylab(label = variable) +
    #geom_label(aes(label=paste(profperc,"%"), x = as.Date(-Inf,origin="1900-01-01"),y = -Inf,fill=ifelse(profperc>0,F,T)),vjust="bottom",hjust="left",data=get(var2),show.legend = F) + 
    geom_label(aes(label= str_sub(bank,1,3), x = as.Date(-Inf,origin="1900-01-01"),y = Inf,fill=bank),hjust = "left",vjust="top",color = "white",show.legend = F) + 
    scale_fill_manual(c("profperc","bank"),values = c("green","blue","saddlebrown","skyblue","red")) # since the labels are sorted alphabteically the FALSE comes at the top and hence green.
}

# use code 45 for Mirae and 56 for L&T
readnav <- function(mf=56,st_date="20180101", end_date="20180901",
                    part1url="http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf=",
                    maxlines=1000){
  mfline=paste0(mf,"&tp=1")
  dt1 <- format(ymd(st_date),"%d-%a-%Y")
  dt2 <- format(ymd(end_date),"%d-%a-%Y")
  part2url <- paste0(mfline,"&frmdt=",st_date,"&todt=",end_date)
  fullurl <- paste0(part1url,part2url)
  x1 <- read_lines(fullurl,n_max = maxlines) %>% 
    grep("201[0-9]$",.,val=T) %>% 
    paste0(collapse = "\n") %>% 
    fread
  names(x1) <- qc(code,title,nav,prchprice,saleprice,dt)
  x1$dt %<>% dmy
  x1
}

downnav <- function(t="30092018110544"){ #turns out the text string suffixed to the URL does not matter.
  x1 <- read.csv(file = paste0("https://www.amfiindia.com/spages/NAVAll.txt?t=",t),sep = ";",stringsAsFactors = F)
  setDT(x1)
  names(x1) <- qc(code,isin,isin2,name,nav,dat)
  x1[!nav==""] -> x1
  x1$dat %<>% as.Date(format="%d-%b-%Y")
  x1$nav %<>% as.numeric()
  x1
}

sold <- function(){
  copy(mf2[order(st_date)]) -> copymf
  copymf[,prevunits:=shift(units),by=.(ISIN,title,folio,CAS)]
  copymf[,prevval:=shift(curvalue),by=.(ISIN,title,folio,CAS)]
  copymf[,soldunits:=prevunits-units]
  copymf[,closed:= ifelse(soldunits==prevunits,T,F)]
  copymf[,soldvalue:= ifelse(closed,prevval,soldunits * NAV)]
  copymf[prevunits!=0]
}
