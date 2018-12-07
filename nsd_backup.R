library(magrittr)
library(lubridate)
library(wrapr)
library(pdftools)
library(readtext)
library(data.table)
library(splitstackshape)
library(stringr)
library(udpipe)
library(googlesheets)

#---- setup variables & options ------
pansm <- "AAOPM4542K"
panrm <- "ABLPM8987G"
sriram <- "1nvbuoZ64jGUnE7vQAJ1aGNQ-CqT4AXJjHJljFDCSNWY"
dir() %>% grep("NSDL",.,va=T) -> files
passwords <- ifelse(grepl("10093",files),pansm,panrm)
options(scipen = 999) # disabling scientific penalty 
options(digits=0)

#---- functions ------

frto <- function(string=NULL,range=NULL){
    string[range[1]:range[2]]
}

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

stline <- function(ftxt=NULL,finfo=NULL,period=NULL,signedon=NULL){
    stlineMFF=NULL;stpageMFF=NULL;
    tot_pages=finfo$pages; fulltext=ftxt
    #if(grepl("2017.07.10 16:38:57",signedon)) browser()
    p=1;confirmed_line<- NA_integer_
    while(p <= tot_pages & is.na(confirmed_line)){
        fulltext[[p]] %>% str_split(pattern = "\n") %>% unlist -> pagetext
        flineMFF=which(str_detect(str_remove_all(pagetext," "),fixed("MutualFundfolios(f)",ignore=T)))
        flineERCP=which(str_detect(str_remove_all(pagetext," "),fixed("TotalExpenseRatio&CommissionPaid",ignore=T)))
        if(length(flineMFF)>0){
            indx <- which(str_detect(str_trim(pagetext),regex("^ISIN")))
            confirmed_line <- flineMFF[which((flineMFF+1) %in% indx)[1]]
            if(!is.na(confirmed_line)) { stlineMFF=confirmed_line; stpageMFF=p }
        }
        p <- p+1
    }
    if(p==tot_pages & is.null(confirmed_line)) message(paste("Could'nt find Mutual Fund token in file signed on:",signedon))
    
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

mfrep<- function(filen=1){
# internal function to filter out the unnatural words picked up in fund name
    fn <- function(var) ifelse(grepl("^[0-9]{5,}|^[0-9]{1,}\\.[0-9]{1,}|NOT|AVAILABLE|SBBN|Total|ISIN|UCC",var) | is.na(var), "", str_trim(var))
    pass <- passwords[filen]
    txt <- pdf_text(pdf = files[filen],upw = pass)
    info <- pdf_info(pdf= files[filen],upw = pass)
    
#--- get header information from NSDL statement----
    header <- getid(fulltext = txt)
    print(paste("Starting processing file no.:", filen,"signed on:",header[3]))
    per<- findp(fulltext = txt)

#---- get row numbers from raw text
    stline(ftxt = txt,finfo = info,period=per,signedon = header[3] ) -> dtr
    #print(dtr)
    stopifnot(is.data.table(dtr))
    
#---- extract header of the MF table -----
    pg_start <- dtr[instance=="startMFF",pgno]
    line_mfheader <- dtr[instance=="startMFF",lineno+1]
    mfheader <- txt[[pg_start]] %>% str_split(pattern = "\n") %>% .[[1]] %>% .[line_mfheader] %>% str_split(.,pattern = "\\s{2,}") %>% .[[1]]
    mfheader2 <- txt[[pg_start]] %>% str_split(pattern = "\n") %>% .[[1]] %>% .[line_mfheader+1] %>% str_split(.,pattern = "\\s{1,}") %>% .[[1]]
    if(length(mfheader)<8) mfheader <- qc(ISIN,title,folio,units,NAV,curvalue) else mfheader <- qc(ISIN,title,folio,units,cpunit,totcost,NAV,curvalue,unrprofit)
    # message("line1:"); print(mfheader)
    # message("line2:"); print(mfheader2)
    
#---- pull raw text out from line number range in dtr ----
    pullout(fulltext = txt,rangedt = dtr) -> extr
    dtraw <- data.table(extr=extr)
    dtraw2 <- cSplit(dtraw,"extr",sep = " ",type.convert = "as.character")
    if(names(dtraw2)[1]=="extr_1") names(dtraw2) %<>% str_replace(pattern = "_([0-9])","_0\\1")
    dtraw3 <- dtraw2[,lapply(.SD,str_remove_all,",")]
    dtraw3 <- dtraw3[!(extr_01=="Page" & grepl("\\d+$",extr_02))]
    dtraw3 <- dtraw3[!(extr_01=="Summary" & grepl("Holdings",extr_02))]

#----- identify the dangling digits (upto 2 digits) of folio number ----
    str_detect(as.matrix(dtraw3),"^[0-9]{1,2}$") %>% matrix(nrow=nrow(dtraw3)) %>% .[,1:8] -> mata  # lgl matrix of dangling numbers 
    str_detect(as.matrix(dtraw3),regex("series",ig=T)) %>% matrix(nrow=nrow(dtraw3)) %>% .[,1:8] -> matb  # lgl matrix of dangling numbers 
    is.na(as.matrix(dtraw3)) -> matc 
    mata & !is.na(mata) -> mata 
    matb & !is.na(matb) -> matb
    which(max.col(mata,ties.method = "first")>1) -> ind1 # index of row numbers if the pure number is NOT the first column.
    grep("^INF",dtraw3$extr_01)+1 -> ind2 #  index of row numbers immediately next to ^INF - dangling digits can only be in next rows.
    which(mata,arr.ind = T)  %>%  data.table -> dt_a
    which(matb,arr.ind = T) %>%   data.table -> dt_b
    which(matc,arr.ind = T) %>%   data.table -> dt_c
    dt_ignore <- dt_b[dt_a,on="row"]
    rows_to_ignore <- ifelse(dt_ignore$col+1==dt_ignore$i.col,dt_ignore$row,NULL)
    intersect(ind1,ind2) -> ind3 #   rows that have dangling digits - common rows in ind1 & ind2
    ind3 <- setdiff(ind3,rows_to_ignore)
    if(length(ind3)>0){
        dtraw3[ind3,dig3:=str_extract(string = extr_03,pattern = "^[0-9]{1,2}$")]
        dtraw3[ind3,dig4:=str_extract(string = extr_04,pattern = "^[0-9]{1,2}$")]
        dtraw3[ind3,dig5:=str_extract(string = extr_05,pattern = "^[0-9]{1,2}$")]
        dtraw3[,dig_append:=ifelse(!is.na(dig4),dig4, ifelse(!is.na(dig5),dig5,""))]
        dtraw3[,dig_append:=shift(dig_append,n = 1,type = "lead")]
        dtraw3[ind3,extr_04:=str_remove(string = extr_04,pattern = "^[0-9]{1,2}$")]
        dtraw3[ind3,extr_05:=str_remove(string = extr_05,pattern = "^[0-9]{1,2}$")]
    }
    
#---- collect the fund name from various wrapping positions (avoiding the first column for the first two rows)    
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
#generate boolean matrix with min 6 consecutive digits or SBBN (for Sundaram non stand folio) for possible folio numbers
    str_detect(as.matrix(dtraw4),"^[0-9]{6,}|SBBN[A-Z0-9]{4,}") %>% matrix(nrow=nrow(dtraw4)) %>% .[,2:10] -> matb 
    matb[which(is.na(matb))] <- F
    max.col(m = matb,ties.method = "first") -> seqm
    dtraw4[,col_seq:=seqm+1]
    dtraw4[, folio := .SD[[col_seq]], 1:nrow(dtraw4)]
    dtraw4$title %>% str_locate(.,"INF") %>% .[,1] -> x1 # to detect location of next INF so we can cut the title till the exact end
    dtraw4$title %>% str_sub(start = 1, end = ifelse(is.na(x1),-1,x1-1)) %>% str_trim() -> dtraw4$title
    #if(nrow(dtraw4[grepl("INF109KA1UX2",extr_01)])>0) browser()
    if(length(ind3)>0) dtraw4[,folio:=paste0(folio,dig_append)]
    message("header of table:"); print(mfheader)
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
    by=1:nrow(dtraw4)] else dtraw5 <- 
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
            by=1:nrow(dtraw4)]
        
    
    suppressWarnings(dtraw5[,lapply(.SD,as.numeric),.SDcols=c(5:length(dtraw5))]) -> dt_numcols
    dtraw6 <- cbind(dtraw5[,c(2:4)],dt_numcols)
    print(paste("file",files[filen],"read in with",nrow(dtraw6),"rows"))
    dtraw6[,st_date:=per[1]][,end_date:=per[2]][,name:=header[1]][,CAS:=header[2]]
}

findp <- function(fulltext=NULL){
    fulltext[[2]] %>% str_split(pattern = "\n") %>% unlist %>% str_trim %>% grep("Statement for the period from",.,value = T) -> tmp
    st_date<- str_replace(tmp,"Statement for the period from (\\d{2}.{9}).+$","\\1") %>% dmy
    end_date<- str_replace(tmp,"Statement for the period from.+(\\d{2}.{9})$","\\1") %>% dmy
    per<- c(st_date,end_date)
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

#--- Main code----
1:NROW(files) %>% purrr:: map(mfrep) %>% rbindlist(fill=T) -> nsdl
#1:3 %>% purrr:: map(mfrep) %>% rbindlist(fill=T) -> nsdl
nsdl[,title:=str_replace_all(string = str_trim(title),pattern = regex("[ -]+"),replacement = " ")]
nsdl[,title:=toupper(str_trim(title))]
nsdl$st_date -> dates1
nsdl[,mnth:=paste(month.abb[month(dates1)],year(dates1),sep="_")]
nsdl[,unit_max:=max(units),by=ISIN]
nsdl[,unit_max_tit:=max(units),by=.(title,name)]
message("NSDL statements read into variable:nsdl")
