###################################################################
## CARD.com Data Science & Analytics Team - 2013
## This R script **only provides functions**. These should be called
## in do.R and other downstream scripts that generate reports.
###################################################################

## Check if required packages are loaded
reqpkg<-c("timeDate","grImport", "parallel")
if (!sum( reqpkg %in% rownames(installed.packages()))==length(reqpkg)){
  stop("One or more required R packages not installed")
} else {
  ## confirm package load success
  loaded<-sapply(reqpkg, require, character.only=T)
  if(sum(loaded)<length(loaded)){
    failed<-names(loaded[loaded==F])
    stop(c("Library load failure: ",failed))
  }
}

## Define financial categories for transactions based on actual amounts
## D = Declined | I = Information | S = Spend | L = Load
## H = Hold | O = Other (Unidentifiable)
fincat<-function(x,tab){
  ta<-tab$transaction_amount[x]
  td<-tab$transaction_description[x]
  rp<-tab$rpid[x]
  tt<-tab$transaction_date[x]
  pb<-tab$post_card_balance[x]
  lb<-lkbal(rp,tt)
  if (grepl(".* - DECLINED$",td)==T) {fc<-"D"} else {
    if (ta==0) {fc<-"I"} else {
      if ((pb - lb < 0) & ( abs(pb + ta - lb) <= 0.03 )) {fc<-"S"} else
        if ((pb - lb > 0) & ((abs(pb - abs(ta) - lb) <= 0.03 ))) {fc<-"L"} else
          if (pb == lb) {fc<-"H"}
      else {fc<-"O"}
    }
  }
  return(fc)  
}

###################################################################
## Functions that retreive rpids and financials by date
## 
###################################################################

# ## all = all issued cards 
# get_rpid_all<-function(d=Sys.timeDate()){
#   x<-card_transactions$rpid[(is.na(card_transactions$rpid)==F) &
#                               #(card_transactions$transaction_description=="New Card")&
#                               (card_transactions$transaction_date<=d)]
#   return(unique(x))
# }
# 
# ## alive = activated card
# get_rpid_alive<-function(d=Sys.timeDate()){
#   istrackable<-get_rpid_all(d)
#   isactivated<-card_transactions$rpid[is.na(card_transactions$rpid)==F & card_transactions$transaction_date<=d & card_transactions$transaction_description=="Activate Card"]
#   isalive<-intersect( istrackable, isactivated )  
#   return(isalive)
# }
# 
# ## skeleton = rpid issued but never activated
# get_rpid_skeletons<-function(d=Sys.timeDate()){
#   istrackable<-get_rpid_all(d)
#   isactivated<-get_rpid_alive(d)
#   isskel<-setdiff( istrackable, isactivated )  
#   return(isskel)      
# }
# 
## loader = activated card that has ever loaded >=5$
get_rpid_loaders<-function(d=Sys.timeDate()){
  loadeventlist<-prax_statusmap$Description[ (prax_statusmap$FCategory == "CIn") & (prax_statusmap$Declined ==F )]
  isalive<-get_rpid_alive(d)
  txn<-card_transactions[(card_transactions$transaction_date<=d) & (card_transactions$rpid %in% isalive) & (card_transactions$transaction_description %in% loadeventlist),]
  isloader<-sapply(unique(txn$rpid), function(x){
    loadeventamounts<-txn$transaction_amount[txn$rpid==x]
    sum(loadeventamounts>=5)>=1    
  })
  ans<-names(isloader)[isloader==T]
  return(ans)   
}
# 
# ## active = all rpids alive with >=$5 bal in last 30days
# get_rpid_active<-function(d=Sys.timeDate()){
#   loadeventlist<-as.vector(prax_statusmap$Description[ prax_statusmap$Declined ==F ])
#   isalive<-get_rpid_alive(d) # vector for atomic input
#   txn<-card_transactions[(as.numeric(diff(c(card_transactions$transaction_date,d)), units="days")<= 30) & (card_transactions$rpid %in% isalive) & (card_transactions$transaction_description %in% loadeventlist),]
#   isactive<-sapply(unique(txn$rpid), function(x) {
#     loadbals<-txn$post_card_balance[txn$rpid==x]
#     sum(loadbals>=5)>=1    
#   })
#   ans<-names(isactive)[isactive==T]
#   return(ans)
# }
# 
# ## customer = active with at least one ACH load >= $100)
# get_rpid_customer<-function(d=Sys.timeDate()){
#   loadeventlist<-"ACH Load / Direct Deposit"
#   isactive<-get_rpid_active(d)
#   txn<-card_transactions[  (card_transactions$rpid %in% isactive) & (card_transactions$transaction_description %in% loadeventlist) & (
#     as.numeric(diff(c(card_transactions$transaction_date,d)), units="days")<= 30),]
#   iscustomer<-sapply(unique(txn$rpid), function(x) {
#     achloads<-txn$transaction_amount[txn$rpid==x]
#     sum(achloads>=100)>=1    
#   })
#   ans<-names(iscustomer)[iscustomer==T]
#   return(ans)
# }


get_rpid_all<-function(d=Sys.timeDate()){
  x<-(card_transactions$rpid[(card_transactions$transaction_date<=d) & (is.na(card_transactions$rpid)==F)])
  return(sort(unique(x)))
}


get_rpid_alive<-function(d=Sys.timeDate()){
  istrackable<-get_rpid_all(d)
  isactivated<-card_transactions$rpid[is.na(card_transactions$rpid)==F & card_transactions$transaction_date<=d & card_transactions$transaction_description=="Activate Card"]
  isalive<-intersect( istrackable, isactivated )  
  return(sort(isalive))
}

get_rpid_active<-function(d=Sys.timeDate()){
  x=card_transactions[is.na(card_transactions$rpid)==F ,c("rpid","transaction_date","post_card_balance")]
  a<-subset(x, as.numeric(d-x$transaction_date, units="days")<=30 )
  ans<-unique(a$rpid[ a$post_card_balance >=5 ])
  return(sort(ans[ans %in% get_rpid_alive(d)])
  )
}

get_rpid_customer<-function(d=Sys.timeDate()){
  x=card_transactions[is.na(card_transactions$rpid)==F,c("rpid","transaction_date","transaction_amount", "transaction_description")]
  a<-subset(x, (as.numeric(d-x$transaction_date, units="days")<=30 ) & (x$transaction_description == "ACH Load / Direct Deposit"))
  ans<-unique(a$rpid[ a$transaction_amount >=100 ])
  return(sort(ans[ans %in% get_rpid_alive(d)]))
}


get_rpid_super<-function(d=Sys.timeDate()){
  x=card_transactions[is.na(card_transactions$rpid)==F,c("rpid","transaction_date","transaction_amount", "transaction_description")]
  a<-subset(x, (as.numeric(d-x$transaction_date, units="days")<=30 ) & (x$transaction_description == "ACH Load / Direct Deposit"))
  loadbyrpid<-aggregate(a$transaction_amount, by=list(a$rpid), sum)
  colnames(loadbyrpid)<-c("rpid","load")
  ans<-loadbyrpid$rpid[loadbyrpid$load>=800]
  return(sort(ans[ans %in% get_rpid_alive(d)]))
}



## skeleton = rpid issued but never activated
get_rpid_skeletons<-function(d=Sys.timeDate()){
  istrackable<-get_rpid_all(d)
  isactivated<-get_rpid_alive(d)
  isskel<-setdiff( istrackable, isactivated )  
  return(isskel)      
}

# ## loader = activated card that has ever loaded >=5$
# get_rpid_loaders<-function(d=Sys.timeDate()){
#   loadeventlist<-prax_statusmap$Description[ (prax_statusmap$FCategory == "CIn") & (prax_statusmap$Declined ==F )]
#   x=card_transactions[is.na(card_transactions$rpid)==F,c("rpid","transaction_date","transaction_amount", "transaction_description")]
#   txn<-subset(x, x$transaction_date<=d & (x$transaction_description %in% loadeventlist) & x$transaction_amount >=5 )
#   ans<-unique(x$rpid[x$rpid %in% get_rpid_alive(d)])
#   return(ans)   
# }

############# New functions above


## ach = loaders who use ACH, ignoring tx <5$)
get_rpid_ach<-function(d=Sys.timeDate()){
  achtx<-card_transactions[ (card_transactions$transaction_date <= d) & (  card_transactions$transaction_description=="ACH Load / Direct Deposit") & (as.numeric(card_transactions$transaction_amount)>=5) ,c("rpid","transaction_amount")]  
  isach<-unique(achtx$rpid[ as.numeric(achtx$transaction_amount)>=5 ])  
  return(isach)
}#End Function

## wu = loaders who use WU, ignoring tx <5$)
get_rpid_wu<-function(d=Sys.timeDate()){
  wutx<-card_transactions[ (card_transactions$transaction_date <= d) & (  card_transactions$transaction_description=="WU Reload") & (as.numeric(card_transactions$transaction_amount)>=5) ,c("rpid","transaction_amount")]  
  iswu<-unique(wutx$rpid[ as.numeric(wutx$transaction_amount)>=5 ])  
  return(iswu)
}#End Function


## ach100 = all loaders with ACH loads >= $100
get_rpid_ach100<-function(d=Sys.timeDate()){
  achtx<-card_transactions[ (card_transactions$transaction_date <= d) & (  card_transactions$transaction_description=="ACH Load / Direct Deposit") ,c("rpid","transaction_amount")]
  isach<-unique(achtx$rpid[ as.numeric(achtx$transaction_amount)>=5 ])
  txn<-card_transactions[(card_transactions$rpid %in% isach) & (card_transactions$transaction_description == "ACH Load / Direct Deposit") ,]
  iscustomer<-sapply(unique(txn$rpid), function(x) {
    achloads<-txn$transaction_amount[txn$rpid==x]
    sum(achloads>=100)>=1    
  })
  ans<-names(iscustomer)[iscustomer==T]
  return(ans)
}


## all cards with nonzero bal on date
get_rpid_nonzerobal<-function(d=Sys.timeDate()){
  x<-card_transactions$rpid[(is.na(card_transactions$rpid)==F) & (card_transactions$post_card_balance > 0) & ( card_transactions$transaction_date<= d)]
  return(unique(x))
}

###################################################################
## Functions that retreive properties of rpids 
## lkbal (rpid, timeDate)
## nkbal (tpid, timeDate)
###################################################################


## lookup the last known account balance, given RPID and Date
lkbal<-function(rpid,dt=Sys.timeDate()){
  tx_rpid<-card_transactions[card_transactions$rpid==rpid,]
  if ( ("Activate Card" %in% tx_rpid$transaction_description) & nrow(tx_rpid) < 2){
    amt<-0
  } else {
    othertxt<-tx_rpid$transaction_date[tx_rpid$transaction_date < dt ]
    if(!length(othertxt)>0){amt<-0} else {
      prevtxtm<-max(othertxt)
      amt<-tail(card_transactions$post_card_balance[ card_transactions$transaction_date == prevtxtm]
                ,1)
    }
  }
  return(amt)
}

## lookup the next known account balance, given RPID and Date
nkbal<-function(rpid,dt=Sys.timeDate()){
  if ( rpid %in% get_rpid_alive(dt) & length(card_transactions$rpid[card_transactions$rpid==rpid]) < 2){
    amt<-0
  } else {
    othertxt<-card_transactions$transaction_date[card_transactions$transaction_date > dt & (card_transactions$rpid == rpid)]
    if(!length(othertxt)>0){amt<-0} else {
      nextxtm<-min(othertxt)
      amt<-tail(
        card_transactions$post_card_balance[(card_transactions$rpid == rpid) & (card_transactions$transaction_date == nextxtm)]
        ,1)
    }
  }
  return(amt)
}



get_vol_ofloads_td<-function(rp,dt=Sys.timeDate()){
  subdf<-card_transactions$transaction_amount[card_transactions$rpid == as.character(rp) & card_transactions$transaction_date <= dt & card_transactions$transaction_description %in% c("ACH Load / Direct Deposit","Retail Reload", "WU Reload") & as.numeric(card_transactions$transaction_amount) >=5]
  amt<-sum(subdf)
  return(amt)
  
}
get_vol_ofach_td<-function(rp,dt=Sys.timeDate()){
  subdf<-card_transactions$transaction_amount[card_transactions$rpid == as.character(rp) & (card_transactions$transaction_date <= dt) & (card_transactions$transaction_description == "ACH Load / Direct Deposit") & as.numeric(card_transactions$transaction_amount) >=5]
  amt<-sum(subdf)
  return(amt)
  
}
get_vol_ofwu_td<-function(rp,dt=Sys.timeDate()){
  subdf<-card_transactions$transaction_amount[card_transactions$rpid == as.character(rp) & (card_transactions$transaction_date <= dt) & (card_transactions$transaction_description == "WU Reload") & as.numeric(card_transactions$transaction_amount) >=5]
  amt<-sum(subdf)
  return(amt)
  
}

###################################################################
## Functions that retreive metrics over time 
##
## floatbal
## gdv
## gdv2
## gdv2_ondate
## gdv2_ach
## gdv2_wu
## gdv2_rr
##
###################################################################

## determine total float on given date 
floatbal<- function( dt=Sys.timeDate() ){
  dt=as.timeDate(dt)  
  userlist<-get_rpid_nonzerobal(dt)
  indate<-dt
  acbals<-sapply(userlist, function(x) {
    lkbal(x, indate)
  })
  ans<-sum(unlist(acbals))
  return(as.numeric(ans))
} 


## gdv(dt) get cumulate gross dollar volume loaded till date
## Requires FinCat assignments -- NOT PRESENTLY IN USE
gdv<-function(dt=Sys.timeDate()) {
  subdf<-card_transactions$transaction_amount[card_transactions$transaction_date <= dt]
  subfc<-txn_cat[card_transactions$transaction_date <= dt]  
  subla<-subdf[subfc=="L"]
  amt<-sum(subla)
  return(amt)
}

## faster version not requiring FinCat assignments
## cumulative gdv till date
gdv2<-function(dt=Sys.timeDate(), gen=NA) {
  if(gen %in% c("M","F","U")){
    subdf<-card_transactions$transaction_amount[card_transactions$transaction_date <= dt & card_transactions$transaction_description %in% c("ACH Load / Direct Deposit","Retail Reload", "WU Reload") &  get_gender(card_transactions$rpid)==gen]
    
  } else {
    subdf<-card_transactions$transaction_amount[card_transactions$transaction_date <= dt & card_transactions$transaction_description %in% c("ACH Load / Direct Deposit","Retail Reload", "WU Reload")]
  }
  amt<-sum(subdf)
  return(amt)
}


gdv2_rpidlist<-function(rpidlist,dt=Sys.timeDate(), gen=NA) {
  card_transactions<-card_transactions[ card_transactions$rpid %in% rpidlist,]
  if(gen %in% c("M","F","U")){
    subdf<-card_transactions$transaction_amount[card_transactions$transaction_date <= dt & card_transactions$transaction_description %in% c("ACH Load / Direct Deposit","Retail Reload", "WU Reload") &  get_gender(card_transactions$rpid)==gen]
    
  } else {
    subdf<-card_transactions$transaction_amount[card_transactions$transaction_date <= dt & card_transactions$transaction_description %in% c("ACH Load / Direct Deposit","Retail Reload", "WU Reload")]
  }
  amt<-sum(subdf)
  return(amt)
}

gdv2_ofdataframe<-function(df, dt=Sys.timeDate(), gen=NA) {
  ctx<-df
  if(gen %in% c("M","F","U")){
    subdf<-ctx$transaction_amount[ctx$transaction_date <= dt & ctx$transaction_description %in% c("ACH Load / Direct Deposit","Retail Reload", "WU Reload") &  get_gender(ctx$rpid)==gen]
    
  } else {
    subdf<-ctx$transaction_amount[ctx$transaction_date <= dt & ctx$transaction_description %in% c("ACH Load / Direct Deposit","Retail Reload", "WU Reload")]
  }
  amt<-sum(subdf)
  return(amt)
}


gdv2_gen<-function(dt=Sys.timeDate(), gen=NA) {
  if(gen %in% names(table(cohortdef$gender))){
    subdf<-card_transactions$transaction_amount[card_transactions$transaction_date <= dt & card_transactions$transaction_description %in% c("ACH Load / Direct Deposit","Retail Reload", "WU Reload") &  get_gender(card_transactions$rpid)==gen]
    
  } else {
    subdf<-card_transactions$transaction_amount[card_transactions$transaction_date <= dt & card_transactions$transaction_description %in% c("ACH Load / Direct Deposit","Retail Reload", "WU Reload")]
  }
  amt<-sum(subdf)
  return(amt)
}

gdv2_chn<-function(dt=Sys.timeDate(), chn=NA) {
  if(chn %in% names(table(cohortdef$chn))){
    subdf<-card_transactions$transaction_amount[card_transactions$transaction_date <= dt & card_transactions$transaction_description %in% c("ACH Load / Direct Deposit","Retail Reload", "WU Reload") &  get_chn(card_transactions$rpid)==chn]
    
  } else {
    subdf<-card_transactions$transaction_amount[card_transactions$transaction_date <= dt & card_transactions$transaction_description %in% c("ACH Load / Direct Deposit","Retail Reload", "WU Reload")]
  }
  amt<-sum(subdf)
  return(as.numeric(amt))
}


## gdv on date 
gdv2_ondate<-function(dt=Sys.timeDate(), gen=NA) {
  if(gen %in% c("M","F","U")){
    subdf<-card_transactions$transaction_amount[as.character(card_transactions$transaction_date, format="%Y-%m-%d") == as.character(dt, format="%Y-%m-%d") & card_transactions$transaction_description %in% c("ACH Load / Direct Deposit","Retail Reload", "WU Reload") & get_gender(card_transactions$rpid)==gen ]
  }else{
    subdf<-card_transactions$transaction_amount[as.character(card_transactions$transaction_date, format="%Y-%m-%d") == as.character(dt, format="%Y-%m-%d") & card_transactions$transaction_description %in% c("ACH Load / Direct Deposit","Retail Reload", "WU Reload")]
  }
  amt<-sum(subdf)
  return(amt)
}


gdv2_ondate_chn<-function(dt=Sys.timeDate(), chn=NA) {
  if(chn %in% names(table(cohortdef$chn))){
    subdf<-card_transactions$transaction_amount[as.character(card_transactions$transaction_date, format="%Y-%m-%d") == as.character(dt, format="%Y-%m-%d") & card_transactions$transaction_description %in% c("ACH Load / Direct Deposit","Retail Reload", "WU Reload") & get_chn(card_transactions$rpid)==chn ]
  }else{
    subdf<-card_transactions$transaction_amount[as.character(card_transactions$transaction_date, format="%Y-%m-%d") == as.character(dt, format="%Y-%m-%d") & card_transactions$transaction_description %in% c("ACH Load / Direct Deposit","Retail Reload", "WU Reload")]
  }
  amt<-sum(subdf)
  return(amt)
}

## Get gdv on a given date for a given monthly cohort "yyyy-mm"
gdv2_ondate_moco<-function(dt=Sys.timeDate(), moco=NA, gen=NA) {
  if(gen %in% c("M","F","U")){
    card_transactions<-card_transactions[get_gender(card_transactions$rpid)==gen,]
  }
  if(moco %in% names(table(get_moco(get_rpid_all())))){
    card_transactions<-card_transactions[get_moco(card_transactions$rpid)==moco,]
  }
  subdf<-card_transactions$transaction_amount[as.character(card_transactions$transaction_date, format="%Y-%m-%d") == as.character(dt, format="%Y-%m-%d") & card_transactions$transaction_description %in% c("ACH Load / Direct Deposit","Retail Reload", "WU Reload")]
  
  amt<-sum(subdf)
  return(amt)
}

## Get gdv on a given date for a given monthly cohort "yyyy-mm"
gdv2_ondate_moco_type<-function(dt=Sys.timeDate(), moco=NA, type="all") {
  ## Set the transaction description scope 
  if(type=="ACH"){typelist<-"ACH Load / Direct Deposit"} 
  else if(type=="WU"){typelist<-"WU Reload"} 
  else if(type=="RR"){typelist<-"Retail Reload"} 
  else {typelist<- c("ACH Load / Direct Deposit","Retail Reload", "WU Reload")}
  
  if(moco %in% names(table(get_moco(get_rpid_all())))){
    subdf<-card_transactions$transaction_amount[as.character(card_transactions$transaction_date, format="%Y-%m-%d") == as.character(dt, format="%Y-%m-%d") & card_transactions$transaction_description %in% typelist & get_moco(card_transactions$rpid)==moco ]
  }else{
    subdf<-card_transactions$transaction_amount[as.character(card_transactions$transaction_date, format="%Y-%m-%d") == as.character(dt, format="%Y-%m-%d") & card_transactions$transaction_description %in% typelist]
  }
  amt<-sum(subdf)
  return(amt)
}

## Monthly cohort analysis of RPID list
gdv2_ondate_rpidlist_moco_type<-function(dt=Sys.timeDate(), rpidlist, moco, type="all") {
  
  ## Set the transaction description scope 
  if(type=="ACH"){typelist<-"ACH Load / Direct Deposit"}
  else if(type=="ATMW"){typelist<-c("ATM Withdrawal", "ATM Withdrawal (Int'l)")}
  else if(type=="ATMF"){typelist<-c(prax_statusmap$Description[prax_statusmap$FCategory=="FIn"& prax_statusmap$Declined==F], "Reverse Fee: Monthly Fee")}
  else if(type=="WU"){typelist<-"WU Reload"} 
  else if(type=="MMF"){typelist<-prax_statusmap$Description[grepl("Monthly Fee", prax_statusmap$Description) & prax_statusmap$Declined==F]}   
  else if(type=="RR"){typelist<-"Retail Reload"} 
  else if(type=="INTC"){typelist<-c("PIN Purchase","PIN Purchase w/ Cash Back","Purchase","Purchase (Int'l)")} 
  else {typelist<- c("ACH Load / Direct Deposit","Retail Reload", "WU Reload")}
  
  if(moco %in% names(table(get_moco(get_rpid_all())))){
    ## Subset rpidlist and transactions to monthly cohort
    rpids<-rpidlist[get_moco(rpidlist)==moco]
    card_transactions<-card_transactions[ card_transactions$rpid %in% rpids,]
    ## 
    subdf<-card_transactions$transaction_amount[as.character(card_transactions$transaction_date, format="%Y-%m-%d") == as.character(dt, format="%Y-%m-%d") & card_transactions$transaction_description %in% typelist]
  }else{
    print("Improper montly cohort definition. Returning all...")
    subdf<-card_transactions$transaction_amount[as.character(card_transactions$transaction_date, format="%Y-%m-%d") == as.character(dt, format="%Y-%m-%d") & card_transactions$transaction_description %in% typelist]
  }
  amt<-sum(subdf)
  return(amt)
}



## cum ACH load till date 
gdv2_ach<-function(dt=Sys.timeDate(), gen=NA) {
  if(gen %in% c("M","F","U")){
    subdf<-card_transactions$transaction_amount[card_transactions$transaction_date <= dt & card_transactions$transaction_description %in% c("ACH Load / Direct Deposit") & get_gender(card_transactions$rpid)==gen]
  } else {
    subdf<-card_transactions$transaction_amount[card_transactions$transaction_date <= dt & card_transactions$transaction_description %in% c("ACH Load / Direct Deposit")]
  }
  amt<-sum(subdf)
  return(amt)
}

## cum WU load till date 
gdv2_wu<-function(dt=Sys.timeDate(), gen=NA) {
  if(gen %in% c("M","F","U")){
    subdf<-card_transactions$transaction_amount[card_transactions$transaction_date <= dt & card_transactions$transaction_description %in% c("WU Reload") & get_gender(card_transactions$rpid)==gen]
  } else {
    subdf<-card_transactions$transaction_amount[card_transactions$transaction_date <= dt & card_transactions$transaction_description %in% c("WU Reload")]
  }
  amt<-sum(subdf)
  return(amt)
}

## cum RETAIL RELOAD load till date 
gdv2_rr<-function(dt=Sys.timeDate()) {
  subdf<-card_transactions$transaction_amount[card_transactions$transaction_date <= dt & card_transactions$transaction_description %in% c("Retail Reload")]
  amt<-sum(subdf)
  return(amt)
}

## gross daily dollar load (vector) till date 
dailyld<-function(dt=Sys.timeDate()){
  subdf<-card_transactions[(card_transactions$transaction_date <= dt) & (card_transactions$transaction_description %in% c("ACH Load / Direct Deposit","Retail Reload", "WU Reload")),]
  ldtab<-as.data.frame(cbind("dt"=as.character(subdf$transaction_date, format="%Y-%m-%d"),"desc"=subdf$transaction_description, "amt"=as.numeric(subdf$transaction_amount)))
  dl<-aggregate(as.numeric(ldtab$amt), list(ldtab$dt), sum, simplify=T)
  return(dl)
}

## gross weekly dollar (vector) till date 
# weeklyld<-function(dt=Sys.timeDate()){
#   subdf<-card_transactions[(card_transactions$transaction_date <= dt) & (card_transactions$transaction_description %in% c("ACH Load / Direct Deposit","Retail Reload", "WU Reload")),]
#   ldtab<-as.data.frame(cbind("dt"=as.character(subdf$transaction_date, format="%G-w%V"),"desc"=subdf$transaction_description, "amt"=as.numeric(subdf$transaction_amount)))
#   dl<-aggregate(as.numeric(ldtab$amt), list(ldtab$dt), sum, simplify=T)
#   return(dl)
# }


## Weekly loads with descriptions
weeklyld_wdesc<-function(dt=Sys.timeDate()){
  subdf<-card_transactions[(card_transactions$transaction_date <= dt) & (card_transactions$transaction_description %in% c("ACH Load / Direct Deposit","Retail Reload", "WU Reload")),]
  ldtab<-as.data.frame(cbind("dt"=as.character(subdf$transaction_date, format="%G-w%V"),"desc"=subdf$transaction_description, "amt"=as.numeric(subdf$transaction_amount)))
  dl<-tapply(as.numeric(ldtab$amt), ldtab[c("dt","desc")], sum)
  return(dl)
}


# weeklyld_rr<-function(dt=Sys.timeDate()){
#   subdf<-card_transactions[(card_transactions$transaction_date <= dt) & (card_transactions$transaction_description %in% c("Retail Reload")),]
#   ldtab<-as.data.frame(cbind("dt"=as.character(subdf$transaction_date, format="%G-w%V"),"desc"=subdf$transaction_description, "amt"=as.numeric(subdf$transaction_amount)))
#   dl<-aggregate(as.numeric(ldtab$amt), list(ldtab$dt), sum, simplify=T)
#   return(dl)
# }
# weeklyld_ach<-function(dt=Sys.timeDate()){
#   subdf<-card_transactions[(card_transactions$transaction_date <= dt) & (card_transactions$transaction_description %in% c("ACH Load / Direct Deposit")),]
#   ldtab<-as.data.frame(cbind("dt"=as.character(subdf$transaction_date, format="%G-w%V"),"desc"=subdf$transaction_description, "amt"=as.numeric(subdf$transaction_amount)))
#   dl<-aggregate(as.numeric(ldtab$amt), list(ldtab$dt), sum, simplify=T)
#   return(dl)
# }
# weeklyld_wu<-function(dt=Sys.timeDate()){
#   subdf<-card_transactions[(card_transactions$transaction_date <= dt) & (card_transactions$transaction_description %in% c( "WU Reload")),]
#   ldtab<-as.data.frame(cbind("dt"=as.character(subdf$transaction_date, format="%G-w%V"),"desc"=subdf$transaction_description, "amt"=as.numeric(subdf$transaction_amount)))
#   dl<-aggregate(as.numeric(ldtab$amt), list(ldtab$dt), sum, simplify=T)
#   return(dl)
# }

# Percentile Rank of a Number in a Vector
perc.rank <- function(x,y) sum(y<x)/length(y)*100


# Days to first ACH
# get_days_toactive(rpid)
get_days_tofirstach<-function(x){
  if (length(card_transactions$transaction_date[ card_transactions$rpid==x & card_transactions$transaction_description=="New Card" ])==0 ) {ans<-NA} else {
    issuedate<-tail(card_transactions$transaction_date[ card_transactions$rpid==x & card_transactions$transaction_description=="New Card" ],1)
    
    if (length(card_transactions$transaction_date[ card_transactions$rpid==x & as.numeric(card_transactions$transaction_amount)>=5 & card_transactions$transaction_description == "ACH Load / Direct Deposit" ])==0 ) {ans<-NA} else {  
      firstloaddate<-min(card_transactions$transaction_date[ card_transactions$rpid==x & as.numeric(card_transactions$transaction_amount)>=5 & card_transactions$transaction_description == "ACH Load / Direct Deposit" ],1)
      ans<-as.numeric(diff(c(issuedate,firstloaddate)), units="days")
    }
  }
  return(ans)
}

# Days to first ACH 100
get_days_tofirstach100<-function(x){
  if (length(card_transactions$transaction_date[ card_transactions$rpid==x & card_transactions$transaction_description=="New Card" ])==0 ) {ans<-NA} else {
    issuedate<-tail(card_transactions$transaction_date[ card_transactions$rpid==x & card_transactions$transaction_description=="New Card" ],1)
    
    if (length(card_transactions$transaction_date[ card_transactions$rpid==x & as.numeric(card_transactions$transaction_amount)>=100 & card_transactions$transaction_description == "ACH Load / Direct Deposit" ])==0 ) {ans<-NA} else {  
      firstloaddate<-min(card_transactions$transaction_date[ card_transactions$rpid==x & as.numeric(card_transactions$transaction_amount)>=100 & card_transactions$transaction_description == "ACH Load / Direct Deposit" ],1)
      ans<-as.numeric(diff(c(issuedate,firstloaddate)), units="days")
    }
  }
  return(ans)
}

# Days to first WU
get_days_tofirstwu<-function(x){
  if (length(card_transactions$transaction_date[ card_transactions$rpid==x & card_transactions$transaction_description=="New Card" ])==0 ) {
    ans<-NA
  } else {
    issuedate<-tail(card_transactions$transaction_date[ card_transactions$rpid==x & card_transactions$transaction_description=="New Card" ],1)
    if (length(card_transactions$transaction_date[ card_transactions$rpid==x & as.numeric(card_transactions$transaction_amount)>=5 & card_transactions$transaction_description == "WU Reload" ])==0 ) {
      ans<-NA
    } else {  
      firstloaddate<-min(card_transactions$transaction_date[ card_transactions$rpid==x & as.numeric(card_transactions$transaction_amount)>=5 & card_transactions$transaction_description == "WU Reload" ],1)
      ans<-as.numeric(diff(c(issuedate,firstloaddate)), units="days")
    }
  }
  return(ans)
}


get_number_ofloads_td<-function(x){
  validtx<-card_transactions$transaction_amount[ card_transactions$rpid==x & card_transactions$transaction_description %in% c("WU Reload", "ACH Load / Direct Deposit") ]>=5
  if (sum(validtx)==0){ans<-NA} else
  {ans=sum(validtx)}
  return(ans)
  
}

get_number_ofachloads_td<-function(x){
  validtx<-card_transactions$transaction_amount[ card_transactions$rpid==x & card_transactions$transaction_description == "ACH Load / Direct Deposit" ]>=5
  if (sum(validtx)==0){ans<-NA} else
  {ans=sum(validtx)}
  return(ans)
  
}

get_number_ofwuloads_td<-function(x){
  validtx<-card_transactions$transaction_amount[ card_transactions$rpid==x & card_transactions$transaction_description == "WU Reload" ]>=5
  if (sum(validtx)==0){ans<-NA} else
  {ans=sum(validtx)}
  return(ans)
  
}


get_range_numberofloads<-function(x){
  ans<-cut(x, breaks=(c(0,3,6,9,Inf)))
  levels(ans)<-c("1-3","4-6","7-9","10+")
  return(ans)
}

get_loader_type<- function(x){
  rplist<-as.vector(x)
  loader_type<-rep(NA,length(rplist))
  loader_type[rplist %in% get_rpid_ach()]<-"ACH"
  loader_type[rplist %in% get_rpid_wu()]<-"WU"
  loader_type[rplist %in% intersect(get_rpid_ach(),get_rpid_wu())]<-"Both"
  return(loader_type)
}

mcsapply <- function(x, f, ...) {
  library(parallel)
  res <- mclapply(x, f, ... )
  simplify2array(res)
}


##################################################
## Card.com Default GGPLOT2 Theme
##################################################
theme_card_bw<-function (base_size = 12, base_family = "") {
  theme_grey(base_size = base_size, base_family = base_family) %+replace% 
    theme(axis.text = element_text(size = rel(0.8)), 
          axis.ticks = element_line(colour = "black"), 
          legend.key = element_rect(colour = "grey80"), 
          panel.background = element_rect(fill = "white", colour = NA), 
          panel.border = element_rect(fill = NA, colour = "black", size=0.5), 
          panel.grid.major = element_line(colour = "black", size = 0.2, linetype="dotted"), 
          panel.grid.minor = element_line(colour = "grey90", size = 0.2), 
          strip.background = element_rect(fill = "grey80", colour = "grey50")
    )
}

##################################################
kpi.ggsave <- function(plt, height= 8.5, width= 11, units="in", ...) {
  filename<-paste(gsub("\\.", "_", plt),".pdf", sep="")
  ggsave(plot=plt, filename=filename, height=height, width=width, dpi=dpi, units=units, path="data/temp/",  ...)
}
##################################################
## Import logo as picture
get_logo<-function(){
  PostScriptTrace("data/reference/logo-new.ps","data/reference/logo-new.xml")
  logo<-readPicture("data/reference/logo-new.xml")
  return(logo)
}
##################################################
## Custom Print for KPIs
kpi.print<-function(obj, ... ){
  lastdata<-max(card_transactions$transaction_date)
  print(obj)
  grid.text(("CONFIDENTIAL") ,x = unit(.01, "npc"), y = unit(.01, "npc"), just = c("left", "bottom"), gp = gpar(fontface = "bold", fontsize = 12, col = "black"))
  grid.text(paste("Last Data:",as.character(lastdata-8*3600, format="%m/%d %H:%M"),"| Analytics:",as.character(Sys.timeDate()-8*3600,format="%m/%d %H:%M"), "PST") ,x = unit(.98, "npc"), y = unit(.01, "npc"), just = c("right", "bottom"), gp = gpar( fontsize = 10, col = "black"))
  grid.picture(get_logo(), 0.05,0.98,0.1,0.05)
}

kpi.grid.arrange<-function( ... ){
  lastdata<-max(card_transactions$transaction_date)
  grid.arrange( ... )
  grid.text(("CONFIDENTIAL") ,x = unit(.01, "npc"), y = unit(.01, "npc"), just = c("left", "bottom"), gp = gpar(fontface = "bold", fontsize = 12, col = "black"))
  grid.text(paste("Last Data:",as.character(lastdata-8*3600, format="%m/%d %H:%M"),"| Analytics:",as.character(Sys.timeDate()-8*3600,format="%m/%d %H:%M"), "PST") ,x = unit(.98, "npc"), y = unit(.01, "npc"), just = c("right", "bottom"), gp = gpar( fontsize = 10, col = "black"))
  grid.picture(get_logo(), 0.05,0.98,0.1,0.05)
}

###########################################################
## Functions that return properties of rpids
##
## get_uid, get_originurl, get_zip, get_income_median, 
## get_age, get_gender, get_camp_id, get_camp_name, get_chn
##
##
###########################################################

## Get UID for given RPID ()  ## Not vectorized yet
get_uid<-function(x) {
  positions<-match(as.character(x), as.character(card_user$rpid))
  uids<-as.character(card_user$uid[positions]) 
  return(uids)}

## Get origin URL for given RPID ()  ## Not vectorized yet
get_originurl<-function(x) {
  positions<-match(as.character(x), as.character(card_user$rpid))
  urls<-as.character(card_user$origin_url[positions])    
  return(urls)}

## Get zip for given RPID ()  
get_zip<-function(x) {
  x<-as.character(x)
  uids<-get_uid(x)
  zips<-card_member$zip[match(as.character(uids),as.character(card_member$uid))]
  return(zips)}

## Get median income for given RPID  
get_income_median<-function(x) {
  zips<-get_zip(x)
  inc_median<-census_income_median$median_income[match(as.character(zips),as.character(census_income_median$zip))]
  return(inc_median)}

## Get age for given RPID 
get_age<-function(x) {
  x<-as.character(x)
  uids<-get_uid(x)
  dob<-card_member$date_of_birth[match(as.character(uids),as.character(card_member$uid))]
  age<-round((Sys.timeDate() -dob)/365)  
  return(as.vector(age))}

## Get gender for given RPID  
get_gender<-function(x) {
  x<-as.character(x)
  uids<-get_uid(x)
  fn<-card_member$first_name[match(as.character(uids),as.character(card_member$uid))]
  gen<-fndb$gender[match(sanitize_fname(fn),fndb$fn)]
  gen[is.na(gen)]<-"U"
  return(as.vector(gen))}

get_first_name<-function(x) {
  x<-as.character(x)
  uids<-get_uid(x)
  fn<-card_member$first_name[match(as.character(uids),as.character(card_member$uid))]
  return(as.vector(fn))}


get_email<-function(x) {
  x<-as.character(x)
  uids<-get_uid(x)
  fn<-card_member$mail[match(as.character(uids),as.character(card_member$uid))]
  return(as.vector(fn))}

get_last_name<-function(x) {
  x<-as.character(x)
  uids<-get_uid(x)
  ln<-card_member$last_name[match(as.character(uids),as.character(card_member$uid))]
  return(as.vector(ln))}


## Get camp_id for given RPID  
get_camp_id<-function(x) {
  x<-as.character(x)
  ans<-card_user$campaign[match(x,card_user$rpid)]
  return(as.vector(ans))}

## Get camppaign_name for given RPID  
get_camp_name<-function(x) {
  x<-as.character(x)
  campid<-card_user$campaign[match(x,card_user$rpid)]
  ans<-card_campaigns$campaign_name[match(campid, card_campaigns$campaign)]
  return(as.vector(ans))}

## Get channel for given rpid  
get_chn<-function(x){
  collectionstart<-"9800009742"
  collectionstart_index<-match(collectionstart, card_user$rpid)
  url<-card_user$origin_url[match(x,card_user$rpid)]
  chn_reg<-(regexec(".*chn=([a-z]*)(&|$)", url))
  chn_id<-sapply(regmatches(url, chn_reg ), function(y) as.character(y[2]))
  chn_id[intersect(which(is.na(chn_id), arr.ind=T), which(as.numeric(x)<as.numeric(collectionstart), arr.ind=T))]<-"pre"
  chn_id[intersect(which(is.na(chn_id), arr.ind=T), which(as.numeric(x)>=as.numeric(collectionstart), arr.ind=T))]<-"org"
  chn_id[ which(card_user$hostname[match(x, card_user$rpid)] %in% c("24.253.81.72","76.91.181.30") )]<-"reord"
  return(as.vector(chn_id))
}


## Get Affiliate ID
get_afid<-function(x){
  x<-as.character(x)
  url<-card_user$origin_url[match(x,card_user$rpid)]
  afid_reg<-(regexec(".*afid=([0-9]*)(&|$)",url))
  afid<-unlist(sapply(regmatches(url, afid_reg ), function(x) as.character(x[2])))
  afid[is.null(afid)]<-NA
  return(as.vector(afid) )  
}





#### Some old functions that still need to be vectorized

## Card Issue Date
get_date_ofissue<- function(x){
  card_transactions<-card_transactions[ card_transactions$transaction_description=="New Card" ,]
  ans<-card_transactions$transaction_date[match(x,card_transactions$rpid)]
  return(ans)
}


## Get monthly cohort of RPID from cohortdef
get_moco<-function(x){
  as.character(get_date_ofissue(x) -8*3600, format = "%Y-%m")  
}



## Days to active
# get_days_toactive(rpid)
get_days_toactive<-function(x){
  if (is.na(get_date_ofissue(x))==T) {ans<-NA} else {
    issuedate<-get_date_ofissue(x)
    
    if (length(card_transactions$transaction_date[ card_transactions$rpid==x & card_transactions$transaction_description=="Activate Card" ])==0 ) {ans<-NA} else {  
      activationdate<-head(card_transactions$transaction_date[ card_transactions$rpid==x & card_transactions$transaction_description=="Activate Card" ],1)
      ans<-as.numeric(diff(c(issuedate,activationdate)), units="days")
    }
  }
  return(ans)
}

## Days to first load
# get_days_toactive(rpid)
get_days_tofirstload<-function(x){
  if (is.na(get_date_ofissue(x))==T) {ans<-NA} else {
    issuedate<-get_date_ofissue(x)
    loadtx<-card_transactions[ card_transactions$rpid==x & card_transactions$transaction_description %in% c("WU Reload", "ACH Load / Direct Deposit") & as.numeric(card_transactions$transaction_amount)>=5, ]
    if (nrow(loadtx)==0 ) {ans<-NA} else {  
      firstloaddate<-min(loadtx$transaction_date)
      ans<-as.numeric(diff(c(issuedate,firstloaddate)), units="days")
    }
  }
  return(ans)
}

## Loading Events as Days from Card Issue
get_load_fromissue<-function(x){
  loads<-x[x$transaction_description %in% c("ACH Load / Direct Deposit", "WU Reload"),]
  issues<-x[x$transaction_description == "New Card",]
  goodrpids<-intersect(loads$rpid, issues$rpid)
  goodrpid_ldt<-sapply(goodrpids, function(y) {
    ldt<-loads$transaction_date[loads$rpid==y & as.numeric(loads$transaction_amount)>=5 ]
    doi<-x$transaction_date[ x$rpid==y & x$transaction_description == "New Card"]
    trunc(as.numeric(diff(c(doi,ldt)), units="days"))
  })
  ans<-unlist(goodrpid_ldt)
  if(length(ans)==0){
    ans2<-NA
  } else {
    days<-1:max(ans)
    ans2<-sapply(days, function(x) sum(ans<=x))
  }
  return(ans2)  
}

## Loading Events as Days from Card Issue
get_loadpu_fromissue<-function(x){
  loads<-x[x$transaction_description %in% c("ACH Load / Direct Deposit", "WU Reload"),]
  if (nrow(loads)==0){ans2<-NA} else {
    issues<-x[x$transaction_description == "New Card",]
    goodrpids<-intersect(loads$rpid, issues$rpid)
    goodrpid_ldt<-sapply(goodrpids, function(y) {
      ldt<-loads$transaction_date[loads$rpid==y & as.numeric(loads$transaction_amount)>=5 ]
      doi<-x$transaction_date[ x$rpid==y & x$transaction_description == "New Card"]
      trunc(as.numeric(diff(c(doi,ldt)), units="days"))
    })
    ans<-unlist(goodrpid_ldt)
    if(length(ans)==0) {ans2<-NA} else {
      days<-1:max(as.numeric(ans))
      ans2<-sapply(days, function(y) sum(ans<=y))/length(goodrpid_ldt)
    }
  }
  return(ans2)
}

## Loading Events as Days from Card Issue ACH NORM
get_load1_fromissue<-function(x){
  loads<-x[x$transaction_description %in% c("ACH Load / Direct Deposit", "WU Reload"),]
  if (nrow(loads)==0){ans2<-NA} else {
    issues<-x[x$transaction_description == "New Card",]
    goodrpids<-intersect(loads$rpid, issues$rpid)
    goodrpid_ldt<-sapply(goodrpids, function(y) {
      ldt<-loads$transaction_date[loads$rpid==y & as.numeric(loads$transaction_amount)>=5 ]
      doi<-x$transaction_date[ x$rpid==y & x$transaction_description == "New Card"]
      trunc(as.numeric(diff(c(doi,ldt)), units="days"))
    })
    ans<-unlist(goodrpid_ldt)
    if(length(ans)==0) {ans2<-NA} else {
      days<-1:max(ans)
      ans2<-sapply(days, function(x) sum(ans<=x))/nrow(loads)*100
    } 
  }
  return(ans2)
}

## Loading Events as Days from Card Issue ACH NORM
get_load2_fromissue<-function(x){
  aloads<-x[x$transaction_description %in% c("ACH Load / Direct Deposit", "WU Reload"),]
  loads<-x[x$transaction_description == "ACH Load / Direct Deposit",]
  issues<-x[x$transaction_description == "New Card",]
  goodrpids<-intersect(loads$rpid, issues$rpid)
  if (length(goodrpids)==0){ans2<-NA} else {    
    goodrpid_ldt<-sapply(goodrpids, function(y) {
      ldt<-loads$transaction_date[loads$rpid==y & as.numeric(loads$transaction_amount)>=5 ]
      doi<-x$transaction_date[ x$rpid==y & x$transaction_description == "New Card"]
      trunc(as.numeric(diff(c(doi,ldt)), units="days"))
    })
    ans<-unlist(goodrpid_ldt)
    if(length(ans)==0) {ans2<-NA} else {
      days<-1:max(ans)
      ans2<-sapply(days, function(x) sum(ans<=x))/nrow(aloads)*100
    }
  }
  return(ans2)
}

## Loading Events as Days from Card Issue ACH NORM
get_load3_fromissue<-function(x){
  aloads<-x[x$transaction_description %in% c("ACH Load / Direct Deposit", "WU Reload"),]
  loads<-x[x$transaction_description == "WU Reload",]
  issues<-x[x$transaction_description == "New Card",]
  goodrpids<-intersect(loads$rpid, issues$rpid)
  if (length(goodrpids)==0){ans2<-NA} else {   
    goodrpid_ldt<-sapply(goodrpids, function(y) {
      ldt<-loads$transaction_date[loads$rpid==y & as.numeric(loads$transaction_amount)>=5 ]
      doi<-x$transaction_date[ x$rpid==y & x$transaction_description == "New Card"]
      trunc(as.numeric(diff(c(doi,ldt)), units="days"))
    })
    ans<-unlist(goodrpid_ldt)
    if(length(ans)==0) {ans2<-NA} else {
      days<-1:max(ans)
      ans2<-sapply(days, function(x) sum(ans<=x))/nrow(aloads)*100
    }
  }
  return(ans2)  
}

## Loading Events as Days from Card Issue ACH NORM
get_load4_fromissue<-function(x){
  aloads<-x[x$transaction_description %in% c("ACH Load / Direct Deposit", "WU Reload"),]
  loads<-x[x$transaction_description == "ACH Load / Direct Deposit",]
  issues<-x[x$transaction_description == "New Card",]
  goodrpids<-intersect(loads$rpid, issues$rpid)
  if (length(goodrpids)==0){ans2<-NA} else {   
    goodrpid_ldt<-sapply(goodrpids, function(y) {
      ldt<-loads$transaction_date[loads$rpid==y & as.numeric(loads$transaction_amount)>=100 ]
      doi<-x$transaction_date[ x$rpid==y & x$transaction_description == "New Card"]
      trunc(as.numeric(diff(c(doi,ldt)), units="days"))
    })
    ans<-unlist(goodrpid_ldt)
    if(length(ans)==0) {ans2<-NA} else {
      days<-1:max(ans)
      ans2<-sapply(days, function(x) sum(ans<=x))/nrow(aloads)*100
    }
  }
  return(ans2)  
}

########################################
## Miscellaneous Functions
##
## cbind.fill
## dedupe_df
## sanitize_fname
## get_fmratio (incompl)
########################################

## CBIND FILL HACK
cbind.fill<-function(...){
  nm <- list(...) 
  nm<-lapply(nm, as.matrix)
  n <- max(sapply(nm, nrow)) 
  do.call(cbind, lapply(nm, function (x) 
    rbind(x, matrix(, n-nrow(x), ncol(x))))) 
}

## Deduplicate data frame
dedupe_df<-function(x) {
  x[!duplicated(x),]
}

## Sanitize firstname for gender matching
sanitize_fname<-function(x){
  n<-casefold(iconv(x, "latin1","ASCII",""), upper=T)
  n<-gsub(" *$|^ *|\\t.*$","",n) # Remove leading and trailing spaces and tabs
  n<-gsub("-|\\.|'"," ",n) # Replace periods, apostrophes and hyphenations
  n<-gsub("^FR |^DR |^CAPT |^MR |^MS |^MRS ","",n) # Replace Father and Doctor prefixes
  y<-strsplit(n," ") # Split name by space
  z<-sapply(y, function(x) {
    if(length(x[nchar(x)>=2])==0){ans<-x} else{
      if(length(x[nchar(x)>=2])>1){ # if compound name
        ans<-x[nchar(x)>=2][1]
      } else {
        ans<-x[nchar(x)>=2]
      }
    }
    return(ans)
  })
  return(z)
}

## Sanitize firstname for gender matching
sanitize_lname<-function(x){
  n<-casefold(iconv(x, "latin1","ASCII",""),upper=T)
  n<-gsub(" *$|^ *|\\t.*$","",n) # Remove leading and trailing spaces and tabs
  n<-gsub("-|\\.|'"," ",n) # Replace periods, apostrophes and hyphenations
  n<-gsub(" JR$| SR$| I$| II$| III$| IV$","",n) # Replace Father and Doctor prefixes
  y<-strsplit(n," ") # Split name by space
  z<-sapply(y, function(x) {
    if(length(x[nchar(x)>=2])>1){ # if compound name
      
      x[nchar(x)>=2][1]
    } else {
      x[nchar(x)>=2]
    }
  })
  return(z)
}



### Get gender ratio for given campaign ID  
# get_fmratio<-function(x) {
#   x<-as.character(x)
#   rpids<-cohortdef$rpid[]
#   fn<-card_member$first_name[match(as.character(uids),as.character(card_member$uid))]
#   gen<-fndb$gender[match(sanitize_fname(fn),fndb$fn)]
#   gen[is.na(gen)]<-"U"
#   return(as.vector(gen))}


## USE CARD.COM API Key for ip lookup
geoIP<-function (x) { 
  options(warn = -1)
  y <- data.frame(t(rep(NA, 11)))
  y <- y[-1, ]
  colnames(y) <- c("ipAddress", "statusCode", "latitude", "longitude", "statusMessage", "countryCode", "countryName", "regionName", "cityName", "zipCode", "timeZone")
  library(rjson)
  temp01 <- paste("http://api.ipinfodb.com/v3/ip-city/?key=0bce56668c14be13af99a3e5f6fb68e20fd18026a1d9ccc38cb535bcfae8a2f4&ip=", x, "&format=json", sep = "", collapse = NULL)
  temp02 <- fromJSON(paste(readLines(temp01), collapse = ""))
  y[1, 1] <- temp02[3]
  y[1, 2] <- temp02[1]
  if (!is.na(as.double(temp02[9]))) {
    y[1, 3] <- as.double(unlist(temp02[9]))
  }
  else {
    y[1, 3] <- 0
  }
  if (!is.na(as.double(temp02[10]))) {
    y[1, 4] <- as.double(unlist(temp02[10]))
  }
  else {
    y[1, 4] <- 0
  }
  y[1, 5] <- temp02[2]
  y[1, 6] <- temp02[4]
  y[1, 7] <- temp02[5]
  y[1, 8] <- temp02[6]
  y[1, 9] <- temp02[7]
  y[1, 10] <- temp02[8]
  y[1, 11] <- temp02[11]
  return(y)
}


## Read table from cardlike db
dbget_table<-function(tabname, dbalias){
  if(! ((class(dbalias)== "character" & length(tabname)==1) & dbalias %in% c("cardlike","cardprod","clikelive","cprodlive","analysis")) ){
    stop("dbget_table needs a DB alias character vector input of length=1")
  } else {
    
    if(! (class(tabname)== "character" & length(tabname)==1)) {
      stop("dbget_table needs a table name character vector input of length=1")
    } else {
      
      ## Check if required packages are loaded
      suppressPackageStartupMessages(reqpkg<-c("DBI", "RMySQL", "timeDate"))
      if (!sum( reqpkg %in% rownames(installed.packages()))==length(reqpkg)){
        stop("One or more R packages not installed")
      }
      
      ## confirm package load success
      loaded<-sapply(reqpkg, require, character.only=T)
      if(sum(loaded)<length(loaded)){
        failed<-names(loaded[loaded==F])
        stop(c("Library load failure: ",failed))
      }
      
      ## FUN: CHECK IF DB CONNECTION WORKS ##
      ## Requires auth group "cardlike" defined in ~/.my.conf
      ## to avoid clear passwd queries to DB.
      ## see ?MySQL help in R for setup details
      
      checkdbconnection<-function(dbalias=dbalias){
        ncon<-length(dbListConnections(dbDriver("MySQL")))
        testcon<-dbConnect(dbDriver("MySQL"), group=dbalias)
        if (! length(dbListConnections(dbDriver("MySQL")))==ncon+1 ){
          constat=F
          stop("Unable to connect to cardlike DB")
        } else{ 
          constat=T
          dbDisconnect(testcon)
        }
        return(constat)
      }
      
      ## Load  MYSQL tables into dataframes 
      print("Loading DB tables into objects ...")
      if (checkdbconnection(dbalias)==T){
        conn<-dbConnect(dbDriver("MySQL"), group=dbalias)
        
        # Get paged query
        query_pre <- "select * from "
        query <- paste(query_pre, tabname, sep="", collapse="")
        rs <- dbSendQuery(conn, query)
        
        #Pg 1
        ans<-fetch(rs, n = 5000000)
        
        #Pg 2
        data_p2<-fetch(rs, n = -1)
        
        # Results table
        ans <- rbind(ans, data_p2)
        
        rm(data_p2)
        dbClearResult(rs)
        
      }
      
      print("Done.")      
      
      ## FUN Close all DB connections
      print(paste("Terminating", sum(sapply(dbListConnections(dbDriver("MySQL")), dbDisconnect)),"DB connections"))
      
      return(ans)
    }
  }
}





## Read table from cardlike db
dbget_cl_table<-function(tabname=NA){
  if(! (class(tabname)== "character" & length(tabname)==1)) {
    stop("dbget_cl_table needs character vector input of length=1")
  } else {
    
    ## Check if required packages are loaded
    reqpkg<-c("DBI", "RMySQL", "timeDate")
    if (!sum( reqpkg %in% rownames(installed.packages()))==length(reqpkg)){
      stop("One or more R packages not installed")
    }
    
    ## confirm package load success
    loaded<-sapply(reqpkg, require, character.only=T)
    if(sum(loaded)<length(loaded)){
      failed<-names(loaded[loaded==F])
      stop(c("Library load failure: ",failed))
    }
    
    ## FUN: CHECK IF DB CONNECTION WORKS ##
    ## Requires auth group "cardlike" defined in ~/.my.conf
    ## to avoid clear passwd queries to DB.
    ## see ?MySQL help in R for setup details
    
    checkdbconnection<-function(){
      ncon<-length(dbListConnections(dbDriver("MySQL")))
      testcon<-dbConnect(dbDriver("MySQL"), group="cardlike")
      if (! length(dbListConnections(dbDriver("MySQL")))==ncon+1 ){
        constat=F
        stop("Unable to connect to cardlike DB")
      } else{ 
        constat=T
        dbDisconnect(testcon)
      }
      return(constat)
    }
    
    ## Load  MYSQL tables into dataframes 
    print("Loading DB tables into objects ...")
    if (checkdbconnection()==T){
      conn_cardlike<-dbConnect(dbDriver("MySQL"), group="cardlike")
      genex<-parse(text=paste("dbReadTable(conn_cardlike,\"", tabname,"\")", sep="", collapse=""))
      ans<-eval(genex)
    } 
    print("Done.")
    ## FUN Close all DB connections
    print(paste("Terminating", sum(sapply(dbListConnections(dbDriver("MySQL")), dbDisconnect)),"DB connections"))
    
    return(ans)
  }
}


## Get query from cardlike db as Data Frame
dbget_cl_query2df<-function(tabname=NA){
  if(! (class(tabname)== "character" & length(tabname)==1)) {
    stop("dbget_cl_query2df needs character vector input of length=1")
  } else {
    
    ## Check if required packages are loaded
    reqpkg<-c("DBI", "RMySQL", "timeDate")
    if (!sum( reqpkg %in% rownames(installed.packages()))==length(reqpkg)){
      stop("One or more R packages not installed")
    }
    
    ## confirm package load success
    loaded<-sapply(reqpkg, require, character.only=T)
    if(sum(loaded)<length(loaded)){
      failed<-names(loaded[loaded==F])
      stop(c("Library load failure: ",failed))
    }
    
    ## FUN: CHECK IF DB CONNECTION WORKS ##
    ## Requires auth group "cardlike" defined in ~/.my.conf
    ## to avoid clear passwd queries to DB.
    ## see ?MySQL help in R for setup details
    
    checkdbconnection<-function(){
      ncon<-length(dbListConnections(dbDriver("MySQL")))
      testcon<-dbConnect(dbDriver("MySQL"), group="cardlike")
      if (! length(dbListConnections(dbDriver("MySQL")))==ncon+1 ){
        constat=F
        stop("Unable to connect to cardlike DB")
      } else{ 
        constat=T
        dbDisconnect(testcon)
      }
      return(constat)
    }
    
    ## Load  MYSQL tables into dataframes 
    print("Loading DB query into objects ...")
    if (checkdbconnection()==T){
      conn_cardlike<-dbConnect(dbDriver("MySQL"), group="cardlike")
      genex<-parse(text=paste("dbGetQuery(conn_cardlike,\"", tabname,"\")", sep="", collapse=""))
      ans<-eval(genex)
    } 
    print("Done.")
    ## FUN Close all DB connections
    print(paste("Terminating", sum(sapply(dbListConnections(dbDriver("MySQL")), dbDisconnect)),"DB connections"))
    
    return(ans)
  }
}



## Get query from cardlike db as Data Frame
dbget_cc_query2df<-function(tabname=NA){
  if(! (class(tabname)== "character" & length(tabname)==1)) {
    stop("dbget_cl_query2df needs character vector input of length=1")
  } else {
    
    ## Check if required packages are loaded
    reqpkg<-c("DBI", "RMySQL", "timeDate")
    if (!sum( reqpkg %in% rownames(installed.packages()))==length(reqpkg)){
      stop("One or more R packages not installed")
    }
    
    ## confirm package load success
    loaded<-sapply(reqpkg, require, character.only=T)
    if(sum(loaded)<length(loaded)){
      failed<-names(loaded[loaded==F])
      stop(c("Library load failure: ",failed))
    }
    
    ## FUN: CHECK IF DB CONNECTION WORKS ##
    ## Requires auth group "cardlike" defined in ~/.my.conf
    ## to avoid clear passwd queries to DB.
    ## see ?MySQL help in R for setup details
    
    checkdbconnection<-function(){
      ncon<-length(dbListConnections(dbDriver("MySQL")))
      testcon<-dbConnect(dbDriver("MySQL"), group="cardprod")
      if (! length(dbListConnections(dbDriver("MySQL")))==ncon+1 ){
        constat=F
        stop("Unable to connect to card.com production DB")
      } else{ 
        constat=T
        dbDisconnect(testcon)
      }
      return(constat)
    }
    
    ## Load  MYSQL tables into dataframes 
    print("Loading DB query into objects ...")
    if (checkdbconnection()==T){
      conn_cardprod<-dbConnect(dbDriver("MySQL"), group="cardprod")
      genex<-parse(text=paste("dbGetQuery(conn_cardprod,\"", tabname,"\")", sep="", collapse=""))
      ans<-eval(genex)
    } 
    print("Done.")
    ## FUN Close all DB connections
    print(paste("Terminating", sum(sapply(dbListConnections(dbDriver("MySQL")), dbDisconnect)),"DB connections"))
    
    return(ans)
  }
}


## Get query from cardlike db as Data Frame
dbget_query2df<-function(tabname=NA,dbalias){
  if(! ((class(dbalias)== "character" & length(tabname)==1) & dbalias %in% c("cardlike","cardprod","clikelive","cprodlive","analysis")) ){
    stop("dbget_table needs a DB alias character vector input of length=1")
  } else {
    
    if(! (class(tabname)== "character" & length(tabname)==1)) {
      stop("dbget_table needs a table name character vector input of length=1")
    } else {
      
      ## Check if required packages are loaded
      reqpkg<-c("DBI", "RMySQL", "timeDate")
      if (!sum( reqpkg %in% rownames(installed.packages()))==length(reqpkg)){
        stop("One or more R packages not installed")
      }
      
      ## confirm package load success
      loaded<-sapply(reqpkg, require, character.only=T)
      if(sum(loaded)<length(loaded)){
        failed<-names(loaded[loaded==F])
        stop(c("Library load failure: ",failed))
      }
      
      ## FUN: CHECK IF DB CONNECTION WORKS ##
      ## Requires auth group "cardlike" defined in ~/.my.conf
      ## to avoid clear passwd queries to DB.
      ## see ?MySQL help in R for setup details
      
      checkdbconnection<-function(dbalias=dbalias){
        ncon<-length(dbListConnections(dbDriver("MySQL")))
        testcon<-dbConnect(dbDriver("MySQL"), group=dbalias)
        if (! length(dbListConnections(dbDriver("MySQL")))==ncon+1 ){
          constat=F
          stop("Unable to connect to DB")
        } else{ 
          constat=T
          dbDisconnect(testcon)
        }
        return(constat)
      }
      
      ## Load  MYSQL tables into dataframes 
      print("Loading DB query into objects ...")
      
      if (checkdbconnection(dbalias)==T){
        conn<-dbConnect(dbDriver("MySQL"), group=dbalias)
        
        # Get paged query
        query <- paste(tabname, sep="", collapse="")
        rs <- dbSendQuery(conn, query)
        
        #Pg 1
        ans<-fetch(rs, n = 5000000)
        
        #Pg 2
        data_p2<-fetch(rs, n = -1)
        
        # Results table
        ans <- rbind(ans, data_p2)
        
        rm(data_p2)
        dbClearResult(rs)
        
      }
      
      print("Done.")
      
      ## FUN Close all DB connections
      print(paste("Terminating", sum(sapply(dbListConnections(dbDriver("MySQL")), dbDisconnect)),"DB connections"))
      
      return(ans)
    }
  }
}



get_refid<-function(x){
  url<-card_user$origin_url[match(x,card_user$rpid)]
  refid_reg<-(regexec(".*refid=([0-9_]*)[&|$]?", url))
  refid<-sapply(regmatches(url, refid_reg ), function(y) as.character(y[2]))
  refid<-gsub("&.*","",refid)
  return(refid)
}

init_card_defaults<-function(getdata=T){
  ## Source upstream scripts
  if(getdata==T){
    source("load.R")
    source("clean.R")
  }
  source("fun.R")
  library("timeDate")
  library("plyr")
  library("reshape2")
  library("ggplot2")
  library("gplots")
  library("RColorBrewer")
  library("gridExtra")
  library("scales")
  library("parallel")
  
  ###################################################################
  ### Define Color Palettes
  #blr<-colorRampPalette(c( "blue2", "red2"))
  #blr<-colorRampPalette(c( "cyan2", "orange2"))
  #blr<-colorRampPalette(c( "cyan4", "red2"))
  blr<-colorRampPalette(c( "cyan4","yellow2", "red3"))
  blyr<-colorRampPalette(c( "cyan4","yellow2", "red3"))
  #blr<-colorRampPalette(c("#9E0142", "#5E4FA2"))
  grys<-colorRampPalette(c( "black", "gray80"))
  #cardblues<-colorRampPalette(c( "midnightblue", "lightskyblue"))
  cardblues<-colorRampPalette(c( "skyblue2", "chartreuse2"))
  
  
  ## Parallel defaults to 2 cores and needs mc.cores to be set explicitly
  options("mc.cores"=detectCores()-1) 
  
  ## Set level definition behavior for data frames
  options("stringsAsFactors"=F)
  
}

## Initialize Google Analytics
init_ga<-function(){
  library(devtools)
  install_github("rga","skardhamar")
  library(rga)
  ## Ensure that the GA token is in this relative position
  rga.open(instance="ga", where="data/reference/google_analytics_oauth_token.Rdata")
  return(ga)
}


#######################################################################################
## Get ad nid when given aid
get_ad_nid<- function(x){
  nids<-fbam_campaigns$nid[match(fbam_ads$cid[match(x,fbam_ads$aid)], fbam_campaigns$cid)]
  nids[is.na(nids)]<-0L
  return(nids)
}


get_ad_name<-function(x){
  ## Generate refid from ad_name (buggy!) 
  #x<-fbam_ads$aid
  adname<-fbam_ads$ad_name[match(x, fbam_ads$aid)]
  adname[grepl("-",adname)]<-NA
  adname[! grepl("[0-9]+", adname)]<-NA
  adname[! grepl("[0-9]{3,}", adname)]<-NA
  refid_reg<-(regexec("^.*([0-9_]*)?$", adname))
  refid<-sapply(regmatches(adname, refid_reg ), function(y) as.character(y[2])) 
  return(refid)
}

get_ad_orders<- function(x){
  ## Get table of all refids from card_user 
  adf<-data.frame(refid=get_ad_name(x), stringsAsFactors=F)
  orders<-data.frame(ord=table(get_refid(card_user$rpid)),stringsAsFactors=F)
  colnames(orders)<-c("refid","ord")
  
  ## Match order information to refids and clean up
  ans<-merge(adf , orders, by="refid", all.x=T)
  ans<-ans[match(get_ad_name(x), ans$refid),]
  ans2<-round(ans$ord)
  return(ans2)
}

get_ad_keyword<- function(x) {
  keywords<-fbam_ads$keyword[match(x,fbam_ads$aid)]
  return(keywords)
}

get_ad_maxbid<- function(x) {
  maxbid<-fbam_ads$max_bid[match(x,fbam_ads$aid)]
  return(maxbid)
}

get_ad_CTR<- function(x) { ## get percentage CTR
  ind<-match(x, fbam_stats$aid)
  ctr<-fbam_stats$clicks[ind]*100/fbam_stats$impressions[ind]
  return(ctr)
}

get_ad_uCTR<- function(x) { ## get percentage CTR
  ind<-match(x, fbam_stats$aid)
  uctr<-fbam_stats$clicks[ind]*100/fbam_stats$unique_impressions[ind]
  return(uctr)
}

get_ad_freq<- function(x) { ## Freq = impressions/unique_impressions
  ind<-match(x, fbam_stats$aid)
  freq<-fbam_stats$impressions[ind]/fbam_stats$unique_impressions[ind]
  return(freq)
}

get_ad_spent<- function(x) {
  ind<-match(x, fbam_stats$aid)
  spent<-round(fbam_stats$spent[ind]/100, 2)
  return(spent)
}

get_ad_clicks<- function(x) {
  ind<-match(x, fbam_stats$aid)
  clk<-fbam_stats$clicks[ind]
  return(clk)
}

get_ad_reach<- function(x) {
  ind<-match(x, fbam_stats$aid)
  reach<-fbam_stats$unique_impressions[ind]
  return(reach)
}

get_ad_conv<- function(x){
  conv<-get_ad_orders(x)*100/get_ad_clicks(x)
  return(conv)
}

get_ad_CPC<- function(x) { ## get percentage CTR
  cpc<-get_ad_spent(x)/get_ad_clicks(x) 
  return(cpc)
}

get_ad_CPO<- function(x){
  cpo<-get_ad_spent(x)/get_ad_orders(x)
  return(cpo)
}

get_ad_CPA<- function(x){
  cpa<-get_ad_CPO(x)*8
  return(cpa)
}

get_ad_campaign_name<-function(x){
  nid<-get_ad_nid(x)
  camp<-card_campaigns$campaign_name[match(nid, card_campaigns$campaign)]
  return(camp)
}

get_ad_target<-function(x){
  targets<-fbam_ads$target_id[match(x,fbam_ads$aid)]
  return(targets)}

## time conversions

conv_unixtotimeDate_gmt<- function(x){
  require(timeDate)
  as.timeDate(ISOdatetime(1970,1,1,0,0,0,tz="GMT")+ as.numeric(x) )
}

conv_unixtotimeDate_local<- function(x){
  require(timeDate)
  as.timeDate(ISOdatetime(1970,1,1,0,0,0,tz="GMT")+ as.numeric(x), FinC="LosAngeles")
}

conv_timeDatetoUNIX<- function(x=Sys.timeDate()){
  require(timeDate)
  if(! class(x)=="timeDate"){
  stop("Input of class timeDate expected")  
  }
  
  ans <- as.vector(as.numeric(x, units="sec"))
  return(ans)
}



## get IP address info from GeoIP  (in package)

## FUN: generate a data frame of GeoIP stats for a vector of IP addresses
ipget_geo<-function(x, metric=NA){
  
  #Check input class
  if(! class(x)=="character"){
    print("Input a character vector of IP addresses")
    stop()
  }
  
  # Sort IP addresses    
  all_ips<-x
  bad_ips_bool<-! grepl("^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$", all_ips )
  bad_ips<-all_ips[bad_ips_bool]
  ips<-unique(all_ips[! bad_ips_bool])
  
  # Create temp file with ip addresses (to speed up system query)
  write.table(data.frame(ips), ".geoip.tmp", row.names=F, col.names=F, quote=F)
  
  # Get IP stats
  geo<-system("sh ipbatch.sh", intern=T)
  
  # Get ASN data
  asn<-system("sh ipbatch-asn.sh", intern=T)
  
  # Remove temp file
  system("rm .geoip.tmp")
  
  # Process geo data and format output data frame
  Sys.setlocale('LC_ALL','C') 
  fin<-ldply(1:length(ips), function(x){
    
    # For unknown IPs
    if(grepl("IP Address not found",geo[x])==T){
      ans<-c("unknown", rep(NA,9))
      names(ans)<-c("country","state","city","zip","lat","long","dma_code","area_code","asn","asn_desc")
    }else{
      
      # For good IP results
      geox<-gsub("^.*: ?","",geo[x])
      asnx<-gsub("^.*: ","",asn[x])
      asnx_num<-gsub(" .*","",asnx)
      asnx_desc<-gsub("^[A-Z0-9]* ?","",asnx)
      geox<-(strsplit(geox  , ", "))[[1]]
      geox<-c(geox,asnx_num,asnx_desc)
      geox<-geox[1:10]    
      names(geox)<-c("country","state","city","zip","lat","long","dma_code","area_code","asn","asn_desc")
      ans<-geox      
    }
    return(c(ip_address=ips[x],ans))
  })
  
  # Populate bad IPs
  if(length(bad_ips>=1)){
    bad_ips_data<-ldply(1:length(bad_ips), function(x){
      ans<-c("bad address", rep(NA,9))
      names(ans)<-c("country","state","city","zip","lat","long","dma_code","area_code","asn","asn_desc")
      ans<-(c(ip_address=bad_ips[x],ans))
      return(ans)
    })
    fin<-rbind(fin,bad_ips_data)
  }
  
  # Reorder results according to input
  fin<-fin[match(all_ips,fin$ip_address),]
  
  # Format output to named vector unless specific metric requested
  
  # Add ipaddress for full output
  
  if( isTRUE(any(is.na(metric))) ){
    final<-fin
  }else{
    # return unnamed vector if specific fields sought
    final<-fin[,metric]
  }
  
  rownames(final)<-NULL  
  return(final)
  
}  


populate <- function(x, time, promo_id){
  
  conn_prod<-dbConnect(dbDriver("MySQL"), group='cardprod')
  
  conn_cl<-dbConnect(dbDriver("MySQL"), group='cardlike')
  
  rpid <- x[1]
  amount <- x[2]
  reason <- x[3]
  
  description = ""
  
  if (reason == 'Two 250+ DD') {
    description = paste("Direct Deposit Load Promotion (i.d.", promo_id, ")", sep="")    
  } else if (reason == 'Load 100') {
    description = paste("Total 100 Load Promotion (i.d.", promo_id, ")", sep="")    
  }
  
  q1 <- "select count(*) from " 
  table1 <- "cl_promotion_history"
  q12 <- " where rpid = "
  q2 <- " and amount = "
  q3 <- " and description = '"
  
  #  query to check if the record exists in cl_promotion_history
  query <- paste(q1, table1, q12, rpid, q2, amount, q3, description, "'", sep = "")
  # print(query)
  ans<-dbGetQuery(conn_cl, query)
  #  print(ans)  
  if (as.numeric(ans) > 0){
    print("record exists in history")
    dbDisconnect(conn_prod)
    dbDisconnect(conn_cl)
    return()
  }
  
  table2 <- "card_credit_queue"
  
  #  query to check if the record exists in card_credit_queue
  query <- paste(q1, table2, q12, rpid, q2, amount, q3, description, "'", sep = "")
  #  print(query)
  ans<-dbGetQuery(conn_prod, query)
  #  print(ans) 
  
  if (as.numeric(ans) > 0){
    print("record exists in queue")
    dbDisconnect(conn_prod)
    dbDisconnect(conn_cl)
    return()
  }
  
  q4 <- "insert into card_credit_queue (rpid, amount, description, created, service_id, transaction_type_id) values ("
  
  query <- paste(q4, rpid, ",", amount, ',"', description, '",', time, ',"BATCH_TRANS_CR","BC"', ")", sep="")
  
  #   print (query)
  
  dbGetQuery(conn_prod, query) 
  
  query <- paste("select qid from ", table2, q12, rpid, q2, amount, q3, description, "'", sep="")
  
  qid <- dbGetQuery(conn_prod, query)
  
  q1 <- "insert into cl_promotion_history (qid, rpid, amount, description, created, promo_id) values (" 
  
  query <- paste(q1, qid, ",", rpid, ",", amount, ',"', description, '",', time, ",", promo_id, ")", sep = "")
  
  #    print (query)
  
  dbGetQuery(conn_cl, query)  
  
  dbDisconnect(conn_prod)
  
  dbDisconnect(conn_cl)
  
}


#######################################################################################
source("fun_i2c.R")
source("fun_fis.R")
print("Functions Loaded.")

## Detach packages not required downstream
# sapply(rev(reqpkg), function(x) detach(pos = match(paste("package", x, sep=":"), search())))
# rm(list=(c("loaded","reqpkg")))