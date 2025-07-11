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
dt_weixiu <- a_1 %>% 
  filter(new_status!="已取消",
         applytype=="寄修/返修",
         productmodel_name %in% c("产成品-吹风机","产成品-电动牙刷"),
         !new_returnstatus %in% c("异常","一检异常"),
#         is.na(laifen_qualityrecordtime),
         !is.na(new_checkon),
#         is.na(new_deliveriedon)
)
weixiu_jiya_fun <- function(dt,ts){
  a <- dt %>% 
    filter(is.na(laifen_servicecompletetime)|laifen_servicecompletetime>=ts,is.na(laifen_qualityrecordtime)|laifen_qualityrecordtime>=ts,new_checkon<ts) %>% 
    mutate(time_long=as.numeric(difftime(ts,new_checkon,units = "hours")),
          time_long_type=if_else(time_long<=20,"正常",if_else(time_long<=36,"预警","超时")))
  a_1 <- a %>% 
    group_by(time_long_type) %>% 
    summarise(n=n(),.groups = "drop") %>% 
    mutate(ts=ts)
  a_2 <- a %>% 
    summarise(n=n()) %>% 
    mutate(time_long_type="总计",ts=ts)
  #return(a)
  return(rbind(a_1,a_2))
}
#test <- weixiu_jiya_fun(dt_weixiu,as.POSIXct("2025-02-25 08:00"))
#clipr::write_clip(test)
ds <- c(seq.POSIXt(as.POSIXct(str_c(as.character(today()-1)," 08:00")),as.POSIXct(str_c(as.character(today()-1)," 22:00")),"hour"),
        seq.POSIXt(as.POSIXct(str_c(as.character(today())," 08:00")),as.POSIXct(str_c(as.character(today())," ",format(now(),"%H"),":00")),"hour"))
#ds <- seq.POSIXt(as.POSIXct(str_c(as.character(today()-1)," 08:00")),as.POSIXct(str_c(as.character(today())," 08:00")),"day")
td <- max(ds) 
#td <- as.POSIXct("2025-03-15 08:00")
yd <- td-days(1)
weixiu_jiya <- map_dfr(ds,~weixiu_jiya_fun(dt_weixiu,.)) %>% 
  mutate(ds=format(ts,format="%Y/%m/%d"),
         hour=format(ts,format="%H"))
####################################################################################################################################
jixiu_order <- c("new_signedon","new_checkon","laifen_servicecompletetime","laifen_qualityrecordtime","new_deliveriedon")
weixiu_fifo_day_fun <- function(dt,ts){
  a <- dt %>% 
    filter(ds1<ts) %>% 
    mutate(time_long=as.numeric(difftime(ts,ds1,units = "hours")),
           time_long_type=if_else((is.na(ds2)|ds2>=ts)&(is.na(ds3)|ds3>=ts),"积压",
                                  if_else(format(ds2,"%Y-%m-%d")==format(ts,"%Y-%m-%d"),"当天完成","已完成")))
  a_dangtian <- a %>% 
    filter(time_long_type=="当天完成") %>% 
    group_by(time_long_type) %>% 
    summarise(n=n(),.groups = "drop") %>% 
    rename("fifo_type"="time_long_type")
  # a_jiya_ <- a %>% 
  #   filter(time_long_type=="积压") %>% 
  #   mutate(ds=ts)
  a_jiya <- a %>% 
    filter(time_long_type=="积压") %>% 
#    mutate(ds=ts)
  group_by(time_long_type) %>%
  summarise(n=n(),.groups = "drop") %>%
  rename("fifo_type"="time_long_type")
  a_dangtian_n <- a_dangtian$n[1]
  a_all <- a %>%
    filter(time_long_type %in% c("积压","当天完成")) %>%
    arrange(ds1) %>%
    mutate(row_number=row_number(),
           fifo_type=if_else(row_number<=a_dangtian_n,"应出库","其它")) %>%
    filter(time_long_type=="当天完成") %>%
    group_by(fifo_type) %>%
    summarise(n=n(),.groups = "drop") %>%
    rbind(a_dangtian) %>%
    rbind(a_jiya) %>%
    mutate(ds=ts)
  return(a_all)
}
weixiu_fifo_fun <- function(dt_api,ds_,dsrange,type,tp){
  ds_1 <- jixiu_order[str_which(jixiu_order,type)-1]
  ds_2 <- jixiu_order[str_which(jixiu_order,type)]
  ds_3 <- if_else(is.na(jixiu_order[str_which(jixiu_order,type)+1]),ds_2,jixiu_order[str_which(jixiu_order,type)+1])
  # createdon_min <- format(ds1,"%Y-%m-%d")
  # createdon_min_sql <- format(ds1-days(30),"%Y-%m-%d")
  # createdon_max <- format(ds2,"%Y-%m-%d")
  # createdon_max_sql <- format(ds2+days(1),"%Y-%m-%d")
  # conn <- dbConnect(MySQL(),dbname="demo",username="LJH",password="ljhyds666",host="172.16.101.2",port=3306)
  # dt <- dbGetQuery(conn,str_c("select * from maintenance_detail_ruiyun where date_format(new_checkon,'%Y-%m-%d %H:%i:%s') between '",createdon_min_sql,"' and '",createdon_max_sql,"'")) %>% 
  #   mutate(across(createdon:new_deliveriedon,as.POSIXct))
  # dbDisconnect(conn)
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
  #test <- weixiu_jiya_fun(dt_weixiu,as.POSIXct("2025-02-20 23:59"))
  #ds_jiya <- seq.POSIXt(as.POSIXct(str_c(createdon_min," 23:59")),as.POSIXct(str_c(createdon_max," 23:59")),"day")
  weixiu_jiya <- map_dfr(ds_,~weixiu_fifo_day_fun(dt_weixiu,.)) %>% 
    pivot_wider(names_from = fifo_type,values_from = n) %>%
    # mutate(ds=as.POSIXct(format(ds,"%Y-%m-%d")),
    #        month=format(ds,format="%Y/%m")) %>%
    #left_join(dt_week,by=c("ds"="ds")) %>%
    group_by_at(vars(all_of(dsrange))) %>%
    summarise(应出库=sum(应出库),当天完成=sum(当天完成),积压=mean(积压),.groups = "drop") %>%
    mutate(fifo=应出库/当天完成)
  return(weixiu_jiya)
}
# weixiu_jiya <- weixiu_fifo_fun(a_1,ds,"ds","laifen_servicecompletetime","jixiu")
# test <- filter(weixiu_jiya,ds==as.POSIXct("2025-03-14 22:00:00"))
#clipr::write_clip(fenjian_jiya)
fenjian_jiya <- weixiu_fifo_fun(a_1,ds,"ds","new_checkon","jixiu")
zhijian_jiya <- weixiu_fifo_fun(a_1,ds,"ds","laifen_qualityrecordtime","jixiu")
fahuo_jiya <- weixiu_fifo_fun(a_1,ds,"ds","new_deliveriedon","jixiu")
#clipr::write_clip(fahuo_jiya)
################################################################################################
#clipr::write_clip(weixiu_jiya)
r_all_warn <- weixiu_jiya[which(weixiu_jiya$ts==td&weixiu_jiya$time_long_type=="总计"),"n"]/weixiu_jiya[which(weixiu_jiya$ts==yd&weixiu_jiya$time_long_type=="总计"),"n"]-1
r_pre_warn <- weixiu_jiya[which(weixiu_jiya$ts==td&weixiu_jiya$time_long_type=="预警"),"n"]/weixiu_jiya[which(weixiu_jiya$ts==yd&weixiu_jiya$time_long_type=="预警"),"n"]-1
r_out_warn <- weixiu_jiya[which(weixiu_jiya$ts==td&weixiu_jiya$time_long_type=="超时"),"n"]/weixiu_jiya[which(weixiu_jiya$ts==td&weixiu_jiya$time_long_type=="总计"),"n"]
v_all_warn <- weixiu_jiya[which(weixiu_jiya$ts==td&weixiu_jiya$time_long_type=="总计"),"n"]
v_pre_warn <- weixiu_jiya[which(weixiu_jiya$ts==td&weixiu_jiya$time_long_type=="预警"),"n"]
v_out_warn <- weixiu_jiya[which(weixiu_jiya$ts==td&weixiu_jiya$time_long_type=="超时"),"n"]

r_fenjian_all_warn <- fenjian_jiya[which(fenjian_jiya$ds==td),"积压"]/fenjian_jiya[which(fenjian_jiya$ds==yd),"积压"]-1
v_fenjian_all_warn <- fenjian_jiya[which(fenjian_jiya$ds==td),"积压"]
r_zhijian_all_warn <- zhijian_jiya[which(zhijian_jiya$ds==td),"积压"]/zhijian_jiya[which(zhijian_jiya$ds==yd),"积压"]-1
v_zhijian_all_warn <- zhijian_jiya[which(zhijian_jiya$ds==td),"积压"]
r_fahuo_all_warn <- fahuo_jiya[which(fahuo_jiya$ds==td),"积压"]/fahuo_jiya[which(fahuo_jiya$ds==yd),"积压"]-1
v_fahuo_all_warn <- fahuo_jiya[which(fahuo_jiya$ds==td),"积压"]


c_all_warn <- if_else(v_all_warn>=3000,"red",if_else(v_all_warn>=2500,"yellow","green"))
c_pre_warn <- if_else(r_pre_warn>=0.2,"red",if_else(r_pre_warn>=0.1,"yellow","green"))
c_out_warn <- if_else(r_out_warn>=0.06,"red",if_else(r_out_warn>=0.03,"yellow","green"))

c_fenjian_all_warn <- if_else(v_fenjian_all_warn>=700,"red",if_else(v_fenjian_all_warn>=500,"yellow","green"))
c_zhijian_all_warn <- if_else(v_zhijian_all_warn>=1200,"red",if_else(v_zhijian_all_warn>=1000,"yellow","green"))
c_fahuo_all_warn <- if_else(v_fahuo_all_warn>=300,"red",if_else(v_fahuo_all_warn>=200,"yellow","green"))

c_warn <- if_else(str_detect(str_c(c_all_warn,c_pre_warn,c_out_warn),"red"),"red",if_else(str_detect(str_c(c_all_warn,c_pre_warn,c_out_warn),"yellow"),"yellow","green"))
#clipr::write_clip(test)
t_all_warn <- if_else(c_all_warn %in% c("red","yellow"),"维修积压量过高","")
t_pre_warn <- if_else(c_pre_warn %in% c("red","yellow"),"维修即将超时量增长明显","")
t_out_warn <- if_else(c_out_warn %in% c("red","yellow"),"维修已超时量占比过高","")
t_warn <- c(t_all_warn,t_pre_warn,t_out_warn)
t_warn <- t_warn[nzchar(t_warn)]
per <- c("11003643","6e4997ed")


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
#   body = list('title' = 'test')
# )
# sheet_token <- content(message)[["data"]][["spreadsheet"]][["spreadsheet_token"]]
# sheet_url <- content(message)[["data"]][["spreadsheet"]][["url"]]
# 
# sheet_share_url <- str_c("https://open.feishu.cn/open-apis/drive/v1/permissions/",sheet_token,"/members/batch_create?need_notification=false&type=sheet")
# message <- POST(
#   url = sheet_share_url,
#   httr::add_headers('Content-Type' = 'application/json',
#                     'Authorization' = str_c('Bearer ',token)),
#   encode = 'json',
#   body = 
#   '{
# 	"members": [
# 		{
# 			"member_id": "11002275",
# 			"member_type": "userid",
# 			"perm": "edit",
# 			"perm_type": "container",
# 			"type": "user"
# 		},
#     {
# 			"member_id": "10201470",
# 			"member_type": "userid",
# 			"perm": "edit",
# 			"perm_type": "container",
# 			"type": "user"
# 		}
# 	]
# }'
# )
# 
# sheet_get_url <- str_c("https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/",sheet_token,"/sheets/query")
# message <- GET(
#   url = sheet_get_url,
#   httr::add_headers('Content-Type' = 'application/json',
#                     'Authorization' = str_c('Bearer ',token)),
#   encode = 'json'
# )
# sheet_id <- content(message)[["data"]][["sheets"]][[1]][["sheet_id"]]
# 
# sheet_write_url <- str_c("https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/",sheet_token,"/values_batch_update") 
# creat_json <- function(...){
#   c(...)
# }
# dt2json <- function(dt,id){
#   dt <- dt %>% 
#     mutate(across(everything(),as.character))
#   lett <- LETTERS[length(dt[1,])]
#   cnt <- nrow(dt)
#   range_title <- str_c(id,"!","A1:",lett,"1")
#   range_dt <- str_c(id,"!","A2:",lett,cnt+1)
#   a <- toJSON(pmap(dt,creat_json))
#   b <- str_c('{
#     "valueRanges": [
#       {
#         "range": "',range_title,'",
#         "values": [
#           ',toJSON(names(dt)),'
#         ]
#       },
#       {
#         "range": "',range_dt,'",
#         "values": ',a,'
#       }
#     ]
#   }')
#   return(b)
# }
# #dt2json(dt_qc,sheet_id)
# 
# message <- POST(
#   url = sheet_write_url,
#   httr::add_headers('Content-Type' = 'application/json',
#                     'Authorization' = str_c('Bearer ',token)),
#   encode = 'json',
#   body = dt2json(dt_qc,sheet_id)
# )
#webhook <- "https://open.feishu.cn/open-apis/bot/v2/hook/559dbd3d-878d-45de-ba06-b0fcdf63b6f8"
webhook <- "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
chat_id <- "oc_c3d8e0e64e1487fc2b07274601826dee"
#chat_id <- "oc_cd60e5dbbb246891c63270d37acfe1e4" #人机
col <- c_warn
time <- as.character(td)
t_all1 <- str_c("<font color='",c_all_warn,"'>",v_all_warn,"单</font>")
t_all2 <- str_c("<text_tag color='",c_all_warn,"'>环比增长 ",round(r_all_warn*100,1),"%</text_tag>")
t_pre1 <- str_c("<font color='",c_pre_warn,"'>",v_pre_warn,"单</font>")
t_pre2 <- str_c("<text_tag color='",c_pre_warn,"'>环比增长 ",round(r_pre_warn*100,1),"%</text_tag>")
t_out1 <- str_c("<font color='",c_out_warn,"'>",v_out_warn,"单</font>")
t_out2 <- str_c("<text_tag color='",c_out_warn,"'>占比 ",round(r_out_warn*100,1),"%</text_tag>")

f_all1 <- str_c("<font color='",c_fenjian_all_warn,"'>",v_fenjian_all_warn,"单</font>")
f_all2 <- str_c("<text_tag color='",c_fenjian_all_warn,"'>环比增长 ",round(r_fenjian_all_warn*100,1),"%</text_tag>")
z_all1 <- str_c("<font color='",c_zhijian_all_warn,"'>",v_zhijian_all_warn,"单</font>")
z_all2 <- str_c("<text_tag color='",c_zhijian_all_warn,"'>环比增长 ",round(r_zhijian_all_warn*100,1),"%</text_tag>")
s_all1 <- str_c("<font color='",c_fahuo_all_warn,"'>",v_fahuo_all_warn,"单</font>")
s_all2 <- str_c("<text_tag color='",c_fahuo_all_warn,"'>环比增长 ",round(r_fahuo_all_warn*100,1),"%</text_tag>")


at <- str_c("<text>",str_c(t_warn,collapse="、"),"，请及时处理~</text><br>",str_c("<at id='",per,"'></at>",collapse=""))
a <- str_c('{ "type": "template", "data": { "template_id": "AAqBcszQRoi4F", "template_version_name": "1.0.8" ,"template_variable": {
            "col": "',col,'",
            "time": "',time,'",
            "t_all1": "',t_all1,'",
            "t_all2": "',t_all2,'",
            "t_pre1": "',t_pre1,'",
            "t_pre2": "',t_pre2,'",
            "t_out1": "',t_out1,'",
            "t_out2": "',t_out2,'",
            "f_all1": "',f_all1,'",
            "f_all2": "',f_all2,'",
            "z_all1": "',z_all1,'",
            "z_all2": "',z_all2,'",
            "s_all1": "',s_all1,'",
            "s_all2": "',s_all2,'",
            "at": "',at,'"
        }} }')
post_body <- list('receive_id' = chat_id,'msg_type' = 'interactive','content' = a)
if(col %in% c("red","yellow")){
  message <- POST(
    url = webhook,
    httr::add_headers('Content-Type' = 'application/json',
                      'Authorization' = str_c('Bearer ',token)),
    encode = 'json',
    body = post_body
  )
}
print(str_c(now(),"已发送"))
# library(taskscheduleR)
# taskscheduler_create(taskname = "feishu_warn_08",
#                      rscript = "C:/Users/徕芬/Desktop/API预警.R",
#                      schedule = "DAILY",
#                      starttime = "08:05",
#                      startdate = format(Sys.Date(), "%Y/%m/%d")
# )
# taskscheduler_create(taskname = "feishu_warn_13",
#                      rscript = "C:/Users/徕芬/Desktop/API预警.R",
#                      schedule = "DAILY",
#                      starttime = "13:35",
#                      startdate = format(Sys.Date(), "%Y/%m/%d")
# )
# taskscheduler_create(taskname = "feishu_warn_18",
#                      rscript = "C:/Users/徕芬/Desktop/API预警.R",
#                      schedule = "DAILY",
#                      starttime = "18:05",
#                      startdate = format(Sys.Date(), "%Y/%m/%d")
# )
# #taskscheduler_stop("myfancyscript")
# taskscheduler_delete(taskname = "feishu_warn_08")