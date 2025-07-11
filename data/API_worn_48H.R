library(httr)
library(jsonlite)
library(uuid)
library(tidyverse)
get_url <- function(api_name,queryid,pageindex,pagesize,isUserQuery,isPreview,orderby,paging,conditions){
  extendConditions <- URLencode(conditions,reserved = TRUE)
  reqid <- UUIDgenerate(use.time = TRUE)
  timestamp <-as.character(round(as.numeric(as.POSIXct(now(),tz="CST"))*1000,0))
  key <- "u7BDpKHA6VSqTScpEqZ4cPKmYVbQTAxgTBL2Gtit"
  sign <- toupper(digest::digest(str_c("AS_department",conditions,orderby,pageindex,pagesize,paging,reqid,"laifen",timestamp,isPreview,isUserQuery,queryid,key),"sha256", serialize = FALSE))
  url <- str_c("https://ap6-openapi.fscloud.com.cn/t/laifen/open/",
               api_name,
               "?$tenant=laifen&$timestamp=",
               timestamp,
               "&$reqid=",
               reqid,
               "&$appid=AS_department",
               "&$orderby=",
               "createdon%20ascending",
               "&queryid=",
               queryid,
               "&isUserQuery=",
               isUserQuery,
               "&isPreview=",
               isPreview,
               "&$pageindex=",
               pageindex,
               "&$pagesize=",
               pagesize,
               "&$paging=",
               paging,
               "&$extendConditions=",
               extendConditions,
               "&$sign=",
               sign)
  return(url)
}
get_data <- function(url){
  res <- VERB("GET", url = url)
  a <- fromJSON(content(res, 'text'))
  a_1 <- a[["Data"]][["Entities"]]
  return(a_1)
}
get_list <- function(api_name,queryid,pageindex,pagesize,isUserQuery,isPreview,orderby,paging,conditions){
  url <- get_url(api_name,queryid,pageindex,pagesize,isUserQuery,isPreview,orderby,paging,conditions)
  res <- VERB("GET", url = url)
  a <- fromJSON(content(res, 'text'))
  a_cnt <- 1:ceiling(a[["Data"]]$TotalRecordCount/5000)
  url_all <- map_chr(a_cnt,~get_url(api_name,queryid,.,pagesize,isUserQuery,isPreview,orderby,paging,conditions))
  a_1 <- map_dfr(url_all,get_data)
  return(a_1)
}
a <- get_list("api/vlist/ExecuteQuery",
              "38c53a54-813f-a0e0-0000-06f40ebdeca5",
              "1",
              "5000",
              "true",
              "false",
              "createdon ascending",
              "true",
              '[{"name":"new_signedon","val":"15","op":"last-x-days"},{"name":"new_signedon","val":"not-null","op":"not-null"}]')
print(str_c(now(),"已获取数据"))
a_1 <- a %>%
  mutate(productmodel_name=pull(new_productmodel_id,name),
         product_name=pull(new_product_id,name),
         applytype=pull(FormattedValues,new_srv_rma_0.new_applytype),
         new_status=new_srv_rma_0.new_status,
         per_name_fenjian=pull(laifen_systemuser2_id,name),
         per_name_yijian=pull(laifen_systemuser_id,name),
         per_name_weixiu=pull(new_srv_workorder_1.new_srv_worker_id,name),
         new_rma_id=pull(new_rma_id,name),
         new_errorclassifly_name=pull(new_errorclassifly_id,name),
         new_error_name=pull(new_error_id,name),
         new_fromsource=pull(FormattedValues,new_srv_rma_0.new_fromsource),
         laifen_jstsalesorderid=new_srv_rma_0.laifen_jstsalesorderid,
         order_id=new_srv_rma_0.laifen_xdorderid
         #laifen_pickuptime=pull(FormattedValues,laifen_pickuptime)
  ) %>% 
  select(new_rma_id,new_fromsource,order_id,productmodel_name,product_name,laifen_productnumber,new_userprofilesn,laifen_jstsalesorderid,new_returnstatus,new_status,applytype,per_name_fenjian,per_name_yijian,per_name_weixiu,createdon,new_signedon,new_checkon,laifen_servicecompletetime,laifen_qualityrecordtime,new_deliveriedon,new_errorclassifly_name,new_error_name) %>% 
  mutate(new_returnstatus=case_when(new_returnstatus==10~"待取件",
                                    new_returnstatus==30~"已签收",
                                    new_returnstatus==60~"已维修",
                                    new_returnstatus==50~"维修中",
                                    new_returnstatus==70~"已质检",
                                    new_returnstatus==40~"已检测",
                                    new_returnstatus==20~"已取件",
                                    new_returnstatus==80~"已一检",
                                    new_returnstatus==90~"异常",
                                    new_returnstatus==100~"一检异常",
                                    new_returnstatus==110~"地址异常",
                                    TRUE~"X"),
         new_status=case_when(new_status=="10"~"待处理",
                              new_status=="50"~"已评价",
                              new_status=="30"~"已完成",
                              new_status=="40"~"已取消",
                              new_status=="20"~"处理中",
                              new_status=="60"~"已检测",
                              new_status=="80"~"异常",
                              new_status=="70"~"已一检",
                              new_status=="90"~"重复待确认",
                              TRUE~"X"),
         createdon=as.POSIXct(str_c(str_sub(createdon,1,10)," ",str_sub(createdon,12,19))),
         #new_pickupendon=as.POSIXct(str_c(str_sub(new_pickupendon,1,10)," ",str_sub(new_pickupendon,12,19))),
         new_signedon=as.POSIXct(str_c(str_sub(new_signedon,1,10)," ",str_sub(new_signedon,12,19))),
         new_checkon=as.POSIXct(str_c(str_sub(new_checkon,1,10)," ",str_sub(new_checkon,12,19))),
         laifen_servicecompletetime=as.POSIXct(str_c(str_sub(laifen_servicecompletetime,1,10)," ",str_sub(laifen_servicecompletetime,12,19))),
         laifen_qualityrecordtime=as.POSIXct(str_c(str_sub(laifen_qualityrecordtime,1,10)," ",str_sub(laifen_qualityrecordtime,12,19))),
         new_deliveriedon=as.POSIXct(str_c(str_sub(new_deliveriedon,1,10)," ",str_sub(new_deliveriedon,12,19)))
  )
# dt_weixiu <- a_1 %>% 
#   filter(new_status!="已取消",
#          applytype=="寄修/返修",
#          productmodel_name %in% c("产成品-吹风机","产成品-电动牙刷"),
#          !new_returnstatus %in% c("异常","一检异常"),
#          #         is.na(laifen_qualityrecordtime),
#          !is.na(new_checkon),
#          is.na(new_deliveriedon)
#   )
# weixiu_jiya_fun <- function(dt,ts,type){
#   a <- dt %>% 
#     filter(is.na(laifen_servicecompletetime)|laifen_servicecompletetime>=ts,is.na(laifen_qualityrecordtime)|laifen_qualityrecordtime>=ts,new_checkon<ts) %>% 
#     mutate(time_long=round(as.numeric(difftime(ts,new_checkon,units = "hours")),1),
#            time_long_type=if_else(time_long<=36,"<36小时",if_else(time_long<=48,"36~48小时","超48小时"))) %>% 
#     arrange(desc(time_long))
#   a_1 <- a %>% 
#     group_by(time_long_type) %>% 
#     summarise(n=n(),.groups = "drop") %>% 
#     mutate(ts=ts)
#   a_2 <- a %>% 
#     summarise(n=n()) %>% 
#     mutate(time_long_type="总计",ts=ts)
#   #return(a)
#   if(type=="sum"){
#     return(rbind(a_1,a_2))
#   } else {
#     return(a)
#   }
# }
# weixiu_jiya_sum <- weixiu_jiya_fun(dt_weixiu,as.POSIXct(str_c(as.character(today())," ",format(now(),"%H"),":00")),"sum")
# weixiu_jiya <- weixiu_jiya_fun(dt_weixiu,as.POSIXct(str_c(as.character(today())," ",format(now(),"%H"),":00")),"detail")
####################################################################################################################################
jixiu_order <- c("new_signedon","new_checkon","laifen_servicecompletetime","laifen_qualityrecordtime","new_deliveriedon")
weixiu_fifo_day_fun <- function(dt,ts,c_hour){
  a <- dt %>% 
    filter(ds1<ts) %>% 
    mutate(time_long=as.numeric(difftime(ts,ds1,units = "hours")),
           time_long_type=if_else((is.na(ds2)|ds2>=ts)&(is.na(ds3)|ds3>=ts),"积压",
                                  if_else(format(ds2,"%Y-%m-%d")==format(ts,"%Y-%m-%d"),"当天完成","已完成")))
  a_jiya <- a %>% 
    filter(time_long_type=="积压") %>% 
    mutate(
      time_long=round(as.numeric(difftime(ts,ds1,units = "hours")),1),
      time_long_type=if_else(time_long<=c_hour[1],str_c("<",c_hour[1],"小时"),if_else(time_long<=c_hour[2],str_c(c_hour[1],"~",c_hour[2],"小时"),str_c("超",c_hour[2],"小时"))))
  return(a_jiya)
}
weixiu_fifo_fun <- function(dt_api,ds_,dsrange,type,tp,sum_type,c_hour){
  ds_1 <- jixiu_order[str_which(jixiu_order,type)-1]
  ds_2 <- jixiu_order[str_which(jixiu_order,type)]
  ds_3 <- if_else(is.na(jixiu_order[str_which(jixiu_order,type)+1]),ds_2,jixiu_order[str_which(jixiu_order,type)+1])
  if(tp=="fenjian"){
    applytype_s <- c("寄修/返修","退货","换货")
  } else {
    applytype_s <- c("寄修/返修")
  }
  if(type=="new_deliveriedon"){
    dt_weixiu <- dt_api %>% 
      rename("ds1":=!!ds_1,"ds2":=!!ds_2,"ds3":=!!ds_3) %>%
      filter(new_status!="已取消",
             applytype %in% applytype_s,
             productmodel_name %in% c("产成品-吹风机","产成品-电动牙刷"),
             !new_returnstatus %in% c("异常","一检异常"),
             !is.na(ds1)) %>% 
      mutate(ds2=ds3)
  } else {
    dt_weixiu <- dt_api %>% 
      rename("ds1":=!!ds_1,"ds2":=!!ds_2,"ds3":=!!ds_3) %>%
      filter(new_status!="已取消",
             applytype %in% applytype_s,
             productmodel_name %in% c("产成品-吹风机","产成品-电动牙刷"),
             !new_returnstatus %in% c("异常","一检异常"),
             !is.na(ds1))
  }
  weixiu_jiya <- weixiu_fifo_day_fun(dt_weixiu,as.POSIXct(str_c(as.character(today())," ",format(now(),"%H"),":00")),c_hour) %>% 
    rename(!!ds_1:="ds1",!!ds_2:="ds2",!!ds_3:="ds3") %>% 
    arrange(desc(time_long))
  names(weixiu_jiya) <- c("单号","单据来源","订单号","产品类别","产品名称","产品编码","序列号","聚水潭售后编码","旧件处理状态",
                          "处理状态","申请类别","分拣人员","一检人员","维修人员","创建时间","签收时间","检测时间","维修完成时间","质检时间",
                          "发货时间","故障类型","故障现象","超时小时","超时类别")
  a_1 <- weixiu_jiya %>% 
    group_by(超时类别) %>% 
    summarise(n=n(),.groups = "drop")
  a_2 <- weixiu_jiya %>% 
    summarise(n=n()) %>% 
    mutate(超时类别="总计")
  weixiu_jiya_sum <- rbind(a_1,a_2)
  title1 <- str_c(c_hour[1],"~",c_hour[2],"小时")
  title2 <- str_c("超",c_hour[2],"小时")
  all1 <- weixiu_jiya_sum[which(weixiu_jiya_sum$超时类别==str_c(c_hour[1],"~",c_hour[2],"小时")),"n"]
  all2 <- weixiu_jiya_sum[which(weixiu_jiya_sum$超时类别==str_c("超",c_hour[2],"小时")),"n"]
  
  dt <- data.frame(title=c(title1,title2),all=as.character(c(all1,all2)))
  if(sum_type=="sum"){
    return(dt)
  } else {
    return(weixiu_jiya)
  }
}
fenjian_jiya_sum <- weixiu_fifo_fun(a_1,ds,"ds","new_checkon","fenjian","sum",c(12,48))
fenjian_jiya <- weixiu_fifo_fun(a_1,ds,"ds","new_checkon","fenjian","detail",c(12,48))
weixiu_jiya_sum <- weixiu_fifo_fun(a_1,ds,"ds","laifen_servicecompletetime","jixiu","sum",c(20,36))
weixiu_jiya <- weixiu_fifo_fun(a_1,ds,"ds","laifen_servicecompletetime","jixiu","detail",c(20,36))
#clipr::write_clip(fahuo_jiya)
################################################################################################
#clipr::write_clip(weixiu_jiya)

#per <- c("11003643","6e4997ed")


library(httr)
library(htmltools)
token_url <- "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
message <- POST(
  url = token_url,
  httr::add_headers('Content-Type' = 'application/json'),
  encode = 'json',
  body = list('app_id' = 'cli_a7e1e09d72fe500e','app_secret' = 'sIsx3hT20I4oQ2lrq0ydAf04LTmaKxP7')
)
token <- content(message)[["tenant_access_token"]]

# creat_sheet_url <- "https://open.feishu.cn/open-apis/sheets/v3/spreadsheets"
# message <- POST(
#   url = creat_sheet_url,
#   httr::add_headers('Content-Type' = 'application/json',
#                     'Authorization' = str_c('Bearer ',token)),
#   encode = 'json',
#   body = list('title' = '维修积压')
# )
# sheet_token2 <- content(message)[["data"]][["spreadsheet"]][["spreadsheet_token"]]
# url2 <- content(message)[["data"]][["spreadsheet"]][["url"]]
# sheet_token1 <- content(message)[["data"]][["spreadsheet"]][["spreadsheet_token"]]
# url1 <- content(message)[["data"]][["spreadsheet"]][["url"]]

# sheet_share_url <- str_c("https://open.feishu.cn/open-apis/drive/v1/permissions/",sheet_token2,"/members/batch_create?need_notification=false&type=sheet")
# message <- POST(
#   url = sheet_share_url,
#   httr::add_headers('Content-Type' = 'application/json',
#                     'Authorization' = str_c('Bearer ',token)),
#   encode = 'json',
#   body =
#     '{
# 	"members": [
# 		{
# 			"member_id": "11002275",
# 			"member_type": "userid",
# 			"perm": "full_access",
# 			"perm_type": "container",
# 			"type": "user"
# 		},
#     {
# 			"member_id": "10201470",
# 			"member_type": "userid",
# 			"perm": "full_access",
# 			"perm_type": "container",
# 			"type": "user"
# 		}
# 	]
# }'
# )
#sheet_token <- "Wi0YsJxS1hXCEBtsRIAcP3FjnRd"
# sheet_get_url <- str_c("https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/NhxbsEaY3hk3ASt7R0dcQcy7nac/sheets/query")
# message <- GET(
#   url = sheet_get_url,
#   httr::add_headers('Content-Type' = 'application/json',
#                     'Authorization' = str_c('Bearer ',token)),
#   encode = 'json'
# )
# test <- content(message)
# sheet_id <- content(message)[["data"]][["sheets"]][[1]][["sheet_id"]]

creat_json <- function(...){
  c(...)
}
dt2json <- function(dt,id){
  dt <- dt %>% 
    mutate(across(everything(),as.character))
  lett <- LETTERS[length(dt[1,])]
  cnt <- nrow(dt)
  range_title <- str_c(id,"!","A1:",lett,"1")
  range_dt <- str_c(id,"!","A2:",lett,cnt+1)
  a <- toJSON(pmap(dt,creat_json))
  b <- str_c('{
    "valueRanges": [
      {
        "range": "',range_title,'",
        "values": [
          ',toJSON(names(dt)),'
        ]
      },
      {
        "range": "',range_dt,'",
        "values": ',a,'
      }
    ]
  }')
  return(b)
}
sheet_ref <- function(token,spreadsheets,sheet_id,dt,type){
  nc <- ncol(dt)
  sheet_add__url <- str_c("https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/",spreadsheets,"/dimension_range")
  message <- POST(
    url = sheet_add__url,
    httr::add_headers('Content-Type' = 'application/json',
                      'Authorization' = str_c('Bearer ',token)),
    encode = 'json',
    body = str_c('{
  "dimension":{
       "sheetId": "',sheet_id,'",
       "majorDimension": "COLUMNS",
       "length": ',nc,'
     }
  }')
  )

  sheet_dele__url <- str_c("https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/",spreadsheets,"/dimension_range")
  message <- DELETE(
    url = sheet_dele__url,
    httr::add_headers('Content-Type' = 'application/json',
                      'Authorization' = str_c('Bearer ',token)),
    encode = 'json',
    body = str_c('{
    "dimension":{
      "sheetId":"',sheet_id,'",
      "majorDimension":"COLUMNS",
      "startIndex":1,
      "endIndex": ',nc,'
    }
  }')
  )
  sheet_write_url <- str_c("https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/",spreadsheets,"/values_batch_update")
  message <- POST(
    url = sheet_write_url,
    httr::add_headers('Content-Type' = 'application/json',
                      'Authorization' = str_c('Bearer ',token)),
    encode = 'json',
    body = dt2json(filter(dt,超时类别==type),sheet_id)
  )
}
sheet_ref(token,"NhxbsEaY3hk3ASt7R0dcQcy7nac","8fa362",fenjian_jiya,"12~48小时")
sheet_ref(token,"NhxbsEaY3hk3ASt7R0dcQcy7nac","MCvE3X",fenjian_jiya,"超48小时")
sheet_ref(token,"UbOVsbkBEhm3IKtWQ72cajA7ncB","SrJUAD",weixiu_jiya,"<20小时")
sheet_ref(token,"UbOVsbkBEhm3IKtWQ72cajA7ncB","107692",weixiu_jiya,"20~36小时")
sheet_ref(token,"UbOVsbkBEhm3IKtWQ72cajA7ncB","aQ3658",weixiu_jiya,"超36小时")


#webhook <- "https://open.feishu.cn/open-apis/bot/v2/hook/559dbd3d-878d-45de-ba06-b0fcdf63b6f8"
webhook <- "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
chat_id <- "oc_27e190a9b63053ea87131a3523ac8509"
#chat_id <- "oc_cd60e5dbbb246891c63270d37acfe1e4" #人机
col <- "yellow"
time <- as.character(format(now(),"%Y-%m-%d %H:%M"))
t_title1 <- fenjian_jiya_sum[1,1]
t_title2 <- fenjian_jiya_sum[2,1]
t_all1 <- str_c("<font color='red'>",fenjian_jiya_sum[1,2],"单</font>")
t_all2 <- str_c("<font color='red'>",fenjian_jiya_sum[2,2],"单</font>")
f_title1 <- weixiu_jiya_sum[1,1]
f_title2 <- weixiu_jiya_sum[2,1]
f_all1 <- str_c("<font color='red'>",weixiu_jiya_sum[1,2],"单</font>")
f_all2 <- str_c("<font color='red'>",weixiu_jiya_sum[2,2],"单</font>")
url1 <- "[分拣超时](https://f1l5e2ythy.feishu.cn/sheets/NhxbsEaY3hk3ASt7R0dcQcy7nac)"
url2 <- "[维修超时](https://f1l5e2ythy.feishu.cn/sheets/UbOVsbkBEhm3IKtWQ72cajA7ncB)"
a <- str_c('{ "type": "template", "data": { "template_id": "AAqBD6tGgvo1O", "template_version_name": "1.0.4" ,"template_variable": {
            "col": "',col,'",
            "time": "',time,'",
            "t_all1": "',t_all1,'",
            "t_all2": "',t_all2,'",
            "f_all1": "',f_all1,'",
            "f_all2": "',f_all2,'",
            "t_title1": "',t_title1,'",
            "t_title2": "',t_title2,'",
            "f_title1": "',f_title1,'",
            "f_title2": "',f_title2,'",
            "url1": "',url1,'",
            "url2": "',url2,'"
        }} }')
post_body <- list('receive_id' = chat_id,'msg_type' = 'interactive','content' = a)
message <- POST(
    url = webhook,
    httr::add_headers('Content-Type' = 'application/json',
                      'Authorization' = str_c('Bearer ',token)),
    encode = 'json',
    body = post_body
)
print(str_c(now(),"已发送"))
# library(taskscheduleR)
# taskscheduler_create(taskname = "feishu48_warn_08",
#                      rscript = "C:/Users/徕芬/Desktop/API预警_48小时.R",
#                      schedule = "DAILY",
#                      starttime = "08:01",
#                      startdate = format(Sys.Date(), "%Y/%m/%d")
# )
# taskscheduler_create(taskname = "feishu48_warn_13",
#                      rscript = "C:/Users/徕芬/Desktop/API预警_48小时.R",
#                      schedule = "DAILY",
#                      starttime = "13:31",
#                      startdate = format(Sys.Date(), "%Y/%m/%d")
# )
# taskscheduler_create(taskname = "feishu48_warn_18",
#                      rscript = "C:/Users/徕芬/Desktop/API预警_48小时.R",
#                      schedule = "DAILY",
#                      starttime = "18:01",
#                      startdate = format(Sys.Date(), "%Y/%m/%d")
# )
# #taskscheduler_stop("myfancyscript")
# taskscheduler_delete(taskname = "feishu48_warn_18")
