getMD5Hash <- function (textToHash) {
  if (!require("digest")) install.packages("digest", repos='http://cran.rstudio.com/')
  stopifnot(library(digest, logical.return = TRUE))
  raw <- iconv(textToHash, from = "", to = "UTF-16LE", toRaw = TRUE)[[1]]
  hash <- toupper(digest(raw, "md5", serialize = FALSE))
  return(hash)
  rm(raw, hash)
}
tokenGenerator <- function (merchantUID, merchantPassword, method1, method2) { 
  if (!require("base64enc")) install.packages("base64enc", repos='http://cran.rstudio.com/')
  stopifnot(library(base64enc, logical.return = TRUE))
  
  # UTC timestamp
  # ToString(DateTimeAdd(DateTimeFormat(DateTimeToUTC(DateTimeNow()), "%Y-%m-%d %H:%M:%S"),-1,"hours"),0) + ".214"
  timestamp <- format(Sys.time() - 3600, tz = "GMT", format = "%Y-%m-%d %H:%M:%OS3", usetz = FALSE)
  
  # UpperCase(MD5_UNICODE(ToString([merchantPassword])))
  # 3D961441DD87BEB66294D32B94860BCA
  merchantPasswordHash <- getMD5Hash(merchantPassword)
  merchantPasswordTimestampMethods <- paste0(merchantPasswordHash, timestamp, method1, ".", method2)
  merchantPasswordTimestampMethodsHash <- getMD5Hash(merchantPasswordTimestampMethods)
  
  # ToString([merchantUID],0)+"|"+[timestamp]+"|"+Uppercase(MD5_UNICODE([merchantPasswordHashUNICODE]+[timestamp]+[method1]+"."+[method2]))
  # 1018838|2015-11-19 00:57:10.214|AE7D7BE6E86058E6D35B29A5226FCB9B
  pre.token <- paste(merchantUID, 
                     timestamp, 
                     merchantPasswordTimestampMethodsHash, 
                     sep = "|")
  token <- base64encode(charToRaw(pre.token))
  
  return(token)
  rm(timestamp, merchantPassword, merchantPasswordTimestampMethods, merchantPasswordTimestampMethodsHash, pre.token, token)
}
getRegisteredAffiliates <- function (merchantUID, merchantPassword) {
  if (!require("httr")) install.packages("httr")
  stopifnot(library(httr, logical.return = TRUE))
  if (!require("jsonlite")) install.packages("jsonlite")
  stopifnot(library(jsonlite, logical.return = TRUE))
  
  method1 <- "affiliateoffers"
  method2 <- "registeredaffiliates"
  methods <- paste0("/", method1, "/", method2)
  
  report <- NULL
  for(m in 1:length(merchantUID)) {
    print(paste0("Fetching ", method2, " for merchant ", merchantUID[m]))
    
    Sys.sleep(15)
    
    token <- tokenGenerator(merchantUID[m], merchantPassword[m], method1, method2)
    # encode strings used for building the URL 
    token <- URLencode(as.character(token))
    
    # compose URL
    # it should look like this: 
    # https://rest.upclick.com/MTAxODgzOHwyMDE1LTExLTEyIDE0OjEwOjQ2LjE3MXw2MUJCQTU4MDU5NDE2RTBCQkUwQUNGNUQ0QkU0QUNBMw==/reports/transactions?startdate=11%2F10%2F2015+00%3A00%3A00&enddate=11%2F10%2F2015+23%3A59%3A59
    url <- NULL
    url <- paste0("https://rest.upclick.com/",
                  token,
                  methods)
    
    print(paste0(Sys.time(), " calling ", url))
    # parse JSON
    temp <- fromJSON(url)
    
    # return on error
    if(!is.null(temp$reports_affiliateoverview$errors)) {
      print(paste0("Error type: ", temp$reports_affiliateoverview$errors$type))
      print(paste0("Error message: ", temp$reports_affiliateoverview$errors$string))
      return("error")
    }
    
    # following should correspond to JSON structure for the chosen method
    temp <- temp$affiliateoffers_registeredaffiliates$affiliate
    
    # when the report contains only one observation, 
    # fromJSON() returns list instead of data frame 
    # when an object from that list is NULL, casting the list to a data frame does not work
    # replacing the NULL objects with "N/A" string resolves the problem
    if(class(temp) == "list") {
      for(n in 1:length(temp)) {
        if(is.null(temp[[n]])) {
          temp[[n]] <- "N/A"
        }
      }
      temp <- data.frame(temp)
    }
    
    if(!is.null(temp)) {
      temp$merchantUID <- merchantUID[m]
      report <- rbind(report, temp)
    }
  }
  
  return(report)
  rm(method1, method2, methods, token, url, report, temp)
}
getReportsTransactions <- function (merchantUID, merchantPassword, date) {
  Sys.sleep(15)
  
  if (!require("httr")) install.packages("httr")
  stopifnot(library(httr, logical.return = TRUE))
  if (!require("jsonlite")) install.packages("jsonlite")
  stopifnot(library(jsonlite, logical.return = TRUE))
  
  method1 <- "reports"
  method2 <- "transactions"
  
  print(paste0("Fetching ", method2, " for merchant ", merchantUID, " for ", date))
  
  methods <- paste0("/", method1, "/", method2)
  
  token <- tokenGenerator(merchantUID, merchantPassword, method1, method2)
  
  # create the date-time variables in a format requried by API
  from.datetime <- format(date, "%m/%d/%Y %H:%M:%S")
  to.datetime <- format(date + 1, "%m/%d/%Y %H:%M:%S")
  
  # encode strings used for building the URL 
  token <- URLencode(as.character(token))
  from.encoded <- URLencode(as.character(from.datetime))
  to.encoded <- URLencode(as.character(to.datetime))
  
  # compose URL
  # it should look like this: 
  # https://rest.upclick.com/MTAxODgzOHwyMDE1LTExLTEyIDE0OjEwOjQ2LjE3MXw2MUJCQTU4MDU5NDE2RTBCQkUwQUNGNUQ0QkU0QUNBMw==/reports/transactions?startdate=11%2F10%2F2015+00%3A00%3A00&enddate=11%2F10%2F2015+23%3A59%3A59
  url <- NULL
  url <- paste0("https://rest.upclick.com/",
                token,
                methods,
                "?startdate=", from.encoded,
                "&enddate=", to.encoded)
  
  # download JSON
  fileName <- paste0("O:/Lulu DWH/External Data Integration/raw-data/Upclick/upclick_", 
                     method1, "_", method2, "_", merchantUID, "_", date,".json")
  response <- NULL
  print(paste0(Sys.time(), " calling ", url))
  response <- GET(url)
  if(!http_error(response)) {
    if(file.exists(fileName)) {
      unlink(fileName)
    }
    writeBin(content(response, "raw"), fileName)
  }
  
  # parse JSON
  report <- NULL
  options(encoding = "UTF-8")
  report <- fromJSON(fileName)
  
  # return on error
  if(!is.null(report$reports_transactions$errors)) {
    print(paste0("Error type: ", report$reports_transactions$errors$type))
    print(paste0("Error message: ", report$reports_transactions$errors$string))
    return("error")
  }
  
  # following should correspond to JSON structure for the chosen method
  if(!is.null(report$reports_transactions)) {
    report <- report$reports_transactions$transactionreport
    report$TransactionDate  <- as.Date(report$TransactionDate)
    report$Firstname <- gsub(pattern = "(^.{200}).*", replacement = "\\1", x = report$Firstname)
    report$Lastname <- gsub(pattern = "(^.{200}).*", replacement = "\\1", x = report$Lastname)
    report$merchantUID <- merchantUID
    
    # Temporary workaround until support for the new field "UpClickReferenceID" is added in the DB
    report$UpClickReferenceID <- NULL
  }
  
  # when the report contains only one observation, 
  # fromJSON returns list instead of data frame 
  # when an object from that list is NULL, casting the list to a data frame does not work
  # replacing the NULL objects with "N/A" string resolves the problem
  if(class(report) == "list") {
    for(n in 1:length(report)) {
      if(is.null(report[[n]])) {
        report[[n]] <- "N/A"
      }
    }
    report <- data.frame(report)
  }
  
  return(report)
}
outersect <- function(x, y) {
  sort(c(setdiff(x, y),
         setdiff(y, x)))
}
addCalculatedFields <- function(x) {
  # --------------- PRODUCT BRAND -------------------------------
  x$Product.Brand <- "others"
  repl <- c("soda", "architect", "pdf suite", "fixio") 
  check <- c("soda|online",
            "architect|forge",
            "suite",
            "fixio")
  for(i in 1:length(check)) {
    x$Product.Brand[regexpr(check[i], x$ProductTitle, ignore.case=TRUE, perl=TRUE) > -1 & x$Product.Brand == "others"] <- repl[i]
  }
  
  #---------------- PRODUCT CATEGORY ---------------------------------------------
  x$Product.Category <- "others"
  repl <- c("pdf", "pc utility") 
  check <- c("soda|online|architect|forge|suite|pdf",
           "pc|fixio")
  for(i in 1:length(check)) {
    x$Product.Category[regexpr(check[i], x$ProductTitle, ignore.case=TRUE, perl=TRUE) > -1 & x$Product.Category == "others"] <- repl[i]
  }

  #---------------- PRODUCT GROUP ---------------------------------------------
  x$Product.Group <- "others"
  repl <- c("soda 5", "soda 6", "soda 7", "soda 8", "soda 9", "soda 3d", "soda others",
           "architect 2", "architect 3", "architect 4", "architect others",
           "pdf suite 2014", "pdf suite 2015", "pdf suite 2016","pdf suite others",
           "fixio cleaner", "fixio optimizer", "fixio driver finder", "fixio others",
           "pc utility", "other pdf", "support", "online services")
  check <- c("(?=.*soda)(?=.*5)",
            "(?=.*soda)(?=.*6)",
            "(?=.*soda)(?=.*7)",
            "(?=.*soda)(?=.*8)",
            "(?=.*soda)(?=.*(home|premium|business|9|anywhere|e\\-sign))",
            "(?=.*soda)(?=.*3d)",
            "soda",
            "(?=.*architect)(?=.*2)",
            "(?=.*architect)(?=.*3)",
            "(?=.*architect)(?=.*4)",
            "architect",
            "(?=.*suite)(?=.*2014)",
            "(?=.*suite)(?=.*2015)",
            "(?=.*suite)(?=.*2016)",
            "suite",
            "(?=.*fixio)(?=.*cleaner)",
            "(?=.*fixio)(?=.*optimizer)",
            "(?=.*fixio)(?=.*driver)",
            "fixio",
            "pc|maximizer|quad|virus|backup|back\\-up|protection|optimizer|plugin|accelerator",
            "pdf",
            "1 year|6 month|1 yr|support|jahr|monate",
            "online service")
  for(i in 1:length(check)) {
    x$Product.Group[regexpr(check[i], x$ProductTitle, ignore.case = TRUE, perl = TRUE) > -1 & x$Product.Group == "others"] <- repl[i]
  }
  
  #------------ CMP GROUP -----------------------------------------------------
  x$Cmp.Group <- 'others'    
  repl <- c("freecreator", "editor", "creator", "converter", "professional", "reader", "writer", "sodapdf", "organic", "direct", "shell", "default", "pdfmerge", "doc2pdf", "pdf", "fpc", "pdfsam", "winzip", "pdftoword", "sumatrapdf", "3d", "download_com", "frams", "bullzip", "pc", "softonic") 
  check <- c("freecreator",
            "editor",
            "creator",
            "converter",
            "profesionnal",
            "reader",
            "writer",
            "sodapdf",
            "organic",
            "direct",
            "shell",
            "default",
            "pdfmerge",
            "doc2pdf",
            "_pdf",
            "fpc",
            "pdfsam",
            "winzip",
            "pdftoword",
            "sumatrapdf",
            "_3d_",
            "download_com",
            "frams",
            "bullzip",
            "pc_",
            "softonic")
  for(i in 1:length(check)) {
    x$Cmp.Group[regexpr(check[i], x$cmp, ignore.case = TRUE, perl = TRUE) > -1 & x$Cmp.Group == "others"] <- repl[i]
  }
  
  #------------------------- CMP TRAFFIC SOURCE -------------------------------------------------------
  x$Cmp.Traffic.Source <- "others"
  repl <- c("google", 
           "bing", 
           "organic", 
           "direct", 
           "default", 
           "shell", 
           "pdfmerge", 
           "pdfsam", 
           "doc2pdf", 
           "softonic", 
           "aed", 
           "cnet") 
  check <- c("_g_",
           "_m_|_msn_",
           "organic",
           "direct",
           "default|N/A",
           "shell",
           "pdfmerge",
           "pdfsam",
           "doc2pdf",
           "softonic",
           "aed",
           "cnet")
  for(i in 1:length(check)) {
    x$Cmp.Traffic.Source[regexpr(check[i], x$cmp, ignore.case = TRUE, perl = TRUE) > -1 & x$Cmp.Traffic.Source == "others"] <- repl[i]
  }
  
  #---------------------------- CHANNELS --------------------------------------------------------------
  x$channels <- "others"
  # email
  x$channels[regexpr('go\\.|get(\\-)?pdf|fixiodriverfinder', x$website, ignore.case = TRUE, perl = TRUE) > -1] <- "email"
  # partners
  x$channels[regexpr("merge|sam|mediaph|ashampoo|softmaker|redbrickmedia|winzip|
                          lu150501avi|wattblicker|meltconsultancy|techlazy|pdfburger|pdfaid|go4convert|
                          docspal|ppc_fr_try|ppc_de_try", x$cmp, ignore.case = TRUE, perl = TRUE) > -1 
                  & x$channels == "others" ] <- "partners"
  # ppc
  x$channels[regexpr("pdf_|fpc|pc_|dls|freelistingpro|softonic|fixio", x$cmp, ignore.case = TRUE, perl = TRUE) > -1 
                  | regexpr("/ppc/|windows|fixio|mypcutility", x$website, ignore.case = TRUE, perl = TRUE) > -1					
                  | (regexpr("pdf\\-suite", x$website, ignore.case = TRUE, perl = TRUE) > -1					
                     & regexpr("default|free", x$cmp, ignore.case = TRUE, perl = TRUE) > -1)
                  | regexpr("pdf\\-suite\\.com/ppc/media/au", x$website, ignore.case = TRUE, perl = TRUE) > -1
                  | (regexpr("pdf\\-format\\.com/uc", x$website, ignore.case = TRUE, perl = TRUE) > -1
                      & regexpr("default", x$cmp, ignore.case = TRUE, perl = TRUE) > -1)
                  |regexpr("pdf\\-reader\\-creator\\.com|pdfsuite\\.de|sodapdf\\.de", x$website, ignore.case = TRUE, perl = TRUE) > -1
                  & x$channels == "others"] <- "ppc"
  
  #organic
  x$channels[regexpr("organic", x$cmp, ignore.case = TRUE, perl = TRUE) > -1 
                  & x$channels == "others"] <- "organic"
  #shell
  x$channels[regexpr("shell", x$cmp, ignore.case = TRUE, perl = TRUE)>-1 
                  & x$channels == "others" ] <- "shell"
  
  # online sales 
  x$channels[regexpr("direct", x$cmp, ignore.case = TRUE, perl = TRUE) > -1 
                  & regexpr("sodapdf\\.com|pdf\\-suite\\.com", x$website, ignore.case = TRUE, perl = TRUE) > -1 
                  & x$channels == "others" ] <- "online sales"
  
  # doc2pdf
  x$channels[regexpr("doc2pdf", x$cmp, ignore.case = TRUE, perl = TRUE) > -1 
                  & x$channels == "others" ] <- "doc2pdf"
  
  #support
  x$channels[regexpr("support", x$cmp, ignore.case = TRUE, perl = TRUE) > -1 
                  & x$channels == "others"] <- "support"
  
  #toolbars
  x$channels[regexpr("toolbar", x$cmp, ignore.case = TRUE, perl = TRUE) > -1 
                  & x$channels == "others"] <- "toolbars"
  
  #--------- AD DISTRIBUTION ----------------------------------------------------------------
  x$AdDistribution <- "others"
  repl <- c("search", "content") 
  check <- c(".*_(s(_.*)?$|search).*",".*_(c(_.*)?$|cont).*")
  for(i in 1:length(check)) {
    x$AdDistribution[regexpr(check[i], x$cmp, ignore.case = TRUE, perl = TRUE) > -1 & x$AdDistribution == "others"] <- repl[i]
  }
  
  #------------------ AdPlatform --------------------------------------------------------------------
  x$AdPlatform <- "others"
  repl <- c("adwords", "bing") 
  check <- c(".*_g_.*",".*_((m|y)(_.*)?$|msn).*")
  for(i in 1:length(check)){
    x$AdPlatform[regexpr(check[i], x$cmp, ignore.case = TRUE, perl = TRUE) > -1 & x$AdPlatform == "others"] <- repl[i]
  }
  
  #------------------- COUNTRIES ---------------------------------------------------------------
  x$Uc.Cmp.Market <- "others"
  check <- c("_us_", "_uk_", "_aus_", "_can_", "_row_", "_it_", "_fr_|_fra_|otherfra", "_br_|_pt_", "_ch_|_de_", "_es_|_sp_","_ru_")
  repl <- c("us", "uk", "aus", "can", "row", "it", "fr", "pt", "de", "es", "ru")
  for(i in 1:length(check)) {
    x$Uc.Cmp.Market[regexpr(check[i], x$cmp, ignore.case = TRUE, perl = TRUE) > -1 & x$Uc.Cmp.Market == "others"] <- repl[i]
  }
  
  return(x)
}
getReportsAffiliateoverview <- function (merchantUID, merchantPassword, date) {
  Sys.sleep(15)
  
  if (!require("httr")) install.packages("httr")
  stopifnot(library(httr, logical.return = TRUE))
  if (!require("jsonlite")) install.packages("jsonlite")
  stopifnot(library(jsonlite, logical.return = TRUE))
  
  method1 <- "reports"
  method2 <- "affiliateoverview"
  
  print(paste0("Fetching ", method2, " for merchant ", merchantUID, " for ", date))
  
  methods <- paste0("/", method1, "/", method2)
  
  token <- tokenGenerator(merchantUID, merchantPassword, method1, method2)
  
  # create the date-time variables in a format requried by API
  from.datetime <- format(date, "%m/%d/%Y %H:%M:%S")
  to.datetime <- format(date + 1, "%m/%d/%Y %H:%M:%S")
  
  # encode strings used for building the URL 
  token <- URLencode(as.character(token))
  from.encoded <- URLencode(as.character(from.datetime))
  to.encoded <- URLencode(as.character(to.datetime))
  
  # compose URL
  # it should look like this: 
  # https://rest.upclick.com/MTAxODgzOHwyMDE1LTExLTEyIDE0OjEwOjQ2LjE3MXw2MUJCQTU4MDU5NDE2RTBCQkUwQUNGNUQ0QkU0QUNBMw==/reports/transactions?startdate=11%2F10%2F2015+00%3A00%3A00&enddate=11%2F10%2F2015+23%3A59%3A59
  url <- NULL
  url <- paste0("https://rest.upclick.com/",
                token,
                methods,
                "?startdate=", from.encoded,
                "&enddate=", to.encoded)
  
  print(paste0("Calling ", url))
  
  # download JSON
  fileName <- paste0("O:/Lulu DWH/External Data Integration/raw-data/Upclick/upclick_", 
                     method1, "_", method2, "_", merchantUID, "_", date,".json")
  response <- NULL
  response <- GET(url)
  if(!http_error(response)) {
    if(file.exists(fileName)) {
      unlink(fileName)
    }
    writeBin(content(response, "raw"), fileName)
  }
  rm(response)
  
  # parse JSON
  report <- NULL
  options(encoding = "UTF-8")
  report <- fromJSON(fileName)
  
  # return on error
  if(!is.null(report$reports_affiliateoverview$errors)) {
    print(paste0("Error type: ", report$reports_affiliateoverview$errors$type))
    print(paste0("Error message: ", report$reports_affiliateoverview$errors$string))
    return("error")
  }
  
  # following should correspond to JSON structure for the chosen method
  if(!is.null(report$reports_affiliateoverview)) {
    report <- report$reports_affiliateoverview$affiliatereport
    report$DayOfTheMonth <- as.Date(report$DayOfTheMonth)
    report$merchantUID <- merchantUID
  }
  
  # when the report contains only one observation, 
  # fromJSON returns list instead of data frame 
  # when an object from that list is NULL, casting the list to a data frame does not work
  # replacing the NULL objects with "N/A" string resolves the problem
  if(class(report) == "list") {
    for(n in 1:length(report)) {
      if(is.null(report[[n]])) {
        report[[n]] <- "N/A"
      }
    }
    report <- data.frame(report)
  }
  return(report)
}
importReportsAffiliateoverview <- function (merchants, dates) {
  if (!require("jsonlite")) install.packages("jsonlite")
  stopifnot(library(jsonlite, logical.return = TRUE))
  
  import <- NULL
  for(m in 1:length(merchants)) {
    for(d in 1:length(dates)) {
      import <- rbind(import, 
                      data.frame(file = paste0("O:/Lulu DWH/External Data Integration/raw-data/Upclick/upclick_reports_affiliateoverview_", 
                                               merchants[m], "_", dates[d], ".json"), 
                                 merchantUID = merchants[m], 
                                 stringsAsFactors = FALSE))
    }
  }
  
  print(paste0("Importing from ", paste0(import$file, collapse = ", ")))
  
  # parse JSON
  options(encoding = "UTF-8")
  report <- NULL
  for(i in 1:nrow(import)) {
    temp <- fromJSON(import$file[i])
    
    # following should correspond to JSON structure for the chosen method
    if(!is.null(temp$reports_affiliateoverview)) {
      temp <- temp$reports_affiliateoverview$affiliatereport
      temp$DayOfTheMonth <- as.Date(temp$DayOfTheMonth)
      temp$merchantUID <- import$merchantUID[i]
      
      report <- rbind(report, temp)
    }
  }
  
  return(report)
}
importReportsTransactions <- function (merchants, dates) {
  if (!require("jsonlite")) install.packages("jsonlite")
  stopifnot(library(jsonlite, logical.return = TRUE))
  
  import <- NULL
  for(m in 1:length(merchants)) {
    for(d in 1:length(dates)) {
      import <- rbind(import, 
                      data.frame(file = paste0("O:/Lulu DWH/External Data Integration/raw-data/Upclick/upclick_reports_transactions_", 
                                               merchants[m], "_", dates[d], ".json"), 
                                 merchantUID = merchants[m], 
                                 stringsAsFactors = FALSE))
    }
  }
  
  # parse JSON
  print(paste0("Importing from ", paste0(import$file, collapse = ", ")))
  options(encoding = "UTF-8")
  report <- NULL
  for(i in 1:nrow(import)) {
    temp <- fromJSON(import$file[i])
    
    # following should correspond to JSON structure for the chosen method
    if(!is.null(temp$reports_transactions)) {
      temp <- temp$reports_transactions$transactionreport
      temp$TransactionDate <- as.Date(temp$TransactionDate)
      temp$Firstname <- gsub(pattern = "(^.{200}).*", replacement = "\\1", x = temp$Firstname)
      temp$Lastname <- gsub(pattern = "(^.{200}).*", replacement = "\\1", x = temp$Lastname)
      temp$merchantUID <- import$merchantUID[i]
      
      # Temporary workaround until support for the new field "UpClickReferenceID" is added in the DB
      temp$UpClickReferenceID <- NULL
      
      report <- rbind(report, temp)
    }
  }
  
  # when the report contains only one observation, 
  # fromJSON returns list instead of data frame 
  # when an object from that list is NULL, casting the list to a data frame does not work
  # replacing the NULL objects with "N/A" string resolves the problem
  if(class(report) == "list") {
    for(n in 1:length(report)) {
      if(is.null(report[[n]])) {
        report[[n]] <- "N/A"
      }
    }
    report <- data.frame(report)
  }
  
  return(report)
}

setwd("O:/Lulu DWH/External Data Integration")
from.date <- as.Date(Sys.Date()-3)
to.date <- as.Date(Sys.Date())
merchant <- c("LULU Software" ,"Interactive Brands")
merchantUID <- c("", "")
merchantPWD <- c("", "")

transactions <- NULL
affiliateoverview <- NULL
for (j in 1:length(merchantUID)) {
  # get affiliates
  aff <- getRegisteredAffiliates(merchantUID = merchantUID[j], merchantPassword = merchantPWD[j])
  aff <- aggregate(ID ~ UID + Username, data = aff, FUN = length)[, c("UID", "Username")]
  aff$UID <- paste0("A", aff$UID)
  
  # loop through each day between "from.date" and "to.date"
  new.date <- from.date
  while(new.date <= to.date) {
    #------------------- transactions ----------------------------
    repeat{
      report <- getReportsTransactions(merchantUID = merchantUID[j], 
                                       merchantPassword = merchantPWD[j], 
                                       date = new.date)
      if(class(report) == "data.frame") {
        break
      }
      Sys.sleep(60)
    }
    if(!is.null(report)) {
      # map affiliates
      report <- merge(x = report, 
                      y = aff, 
                      by.x = "Affiliate", 
                      by.y = "UID", 
                      all.x = TRUE)
      names(report)[names(report) == "Username"] <- "AffiliateName"
      
      # add calculated fields
      report <- addCalculatedFields(report)
      
      transactions <- rbind(transactions, report)
    }
    
    #------------------- affiliateoverview ----------------------------
    repeat{
      report <- getReportsAffiliateoverview(merchantUID = merchantUID[j], 
                                            merchantPassword = merchantPWD[j], 
                                            date = new.date)
      if(class(report) == "data.frame") {
        break
      }
      Sys.sleep(60)
    }
    if(!is.null(report)) {
      affiliateoverview <- rbind(affiliateoverview, report)
    }
    
    # increment "from.date"
    new.date <- new.date + 1
  }
}
# needed for SSIS
column_order <- c("TransactionType", "TransactionDate", "GlobalOrderID", "IP", "Firstname", "Lastname", "Email", "CountryISO", 
                  "CurrencyISO", "Culture", "Address", "Zip", "StateISO", "City", "ProductID", "mSKU", "BaseProductID", 
                  "ProductTitle", "ProductPrice", "ProductPriceUSD", "ProductLevel", "Quantity", "LicenseType", "LicenseKey", 
                  "Affiliate", "affkey1", "affkey2", "cmp", "key1", "key2", "Ref", "Referrer", "PayType", "Em", "NextRebillDate", 
                  "TaxAmount", "TaxAmountUSD", "TaxName", "TaxType", "B2BCompanyName", "B2BCompanyTaxNumber", "Fees", "merchantUID", 
                  "Product.Brand", "Product.Category", "Product.Group", "Cmp.Group", "Cmp.Traffic.Source", "channels", "AdDistribution", 
                  "AdPlatform", "Uc.Cmp.Market", "AffiliateName")
write.csv(x = transactions[, column_order], file = "upclick_transactions.csv", 
          row.names = FALSE, fileEncoding = "UTF-8")
write.csv(x = affiliateoverview, file = "upclick_affiliateoverview.csv", 
          row.names = FALSE, fileEncoding = "UTF-8")