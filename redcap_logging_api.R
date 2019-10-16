library(redcapAPI)

source("tokens.R")

# Connect to REDCap project and export project field names
project           <- kAPITokenNIG
redcap.connection <- redcapConnection(kREDCapAPIURL, project)
field.names       <- exportFieldNames(redcap.connection)

# Read logging file
kLoggingFileName <- "TIPTOPHHSMidlineNigeria_Logging_2019-10-07_1306.csv"
logging.file     <- read.csv(kLoggingFileName, stringsAsFactors = F)


# Build record history ---------------------------------------------------------
kCreatedRecordAction <- "Created Record"
kUpdatedRecordAction <- "Updated Record"

# Filter logs related to record creation and update
created.records <- 
  logging.file[startsWith(logging.file$Action, kCreatedRecordAction), ]
updated.records <- 
  logging.file[startsWith(logging.file$Action, kUpdatedRecordAction), ]

# Extract relevant data from columns
kEndsWithNumberPattern  <- ".* ([0-9]+)"        # Record ID  @ Action column
kContainsAPIPattern     <- "(API)"              # Interface  @ Action column
kContainsAutoPattern    <- "(Auto calculation)" # Auto Calc. @ Action column

# Extract Record ID from Action column
created.records$record_id <- sub(
  pattern     = kEndsWithNumberPattern, 
  replacement = "\\1", 
  x           = created.records$Action
)
updated.records$record_id <- sub(
  pattern     = kEndsWithNumberPattern, 
  replacement = "\\1", 
  x           = updated.records$Action
)

# Extract interface from Action column
created.records$api <- grepl(kContainsAPIPattern, created.records$Action)
updated.records$api <- grepl(kContainsAPIPattern, updated.records$Action)

# Extract auto calculation from Action column
created.records$auto <- grepl(kContainsAutoPattern, created.records$Action)
updated.records$auto <- grepl(kContainsAutoPattern, updated.records$Action)

# Extract relevant data from list of data changes
kTIPTOPHHSMidlineDistrict    <- ".*district = '(\\d)'.*"
kTIPTOPHHSMidlineCluster     <- ".*cluster_.*? = '([0-9]+)'.*"
kTIPTOPHHSMidlineInterviewer <- ".*interviewer_id = '([a-zA-Z0-9_-]*)'.*"
kTIPTOPHHSMidlineDate <- ".*interview_date = '(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})'.*"

ExtractFromLoggingDataChanges <- function(logging.file, column, pattern) {
  # Extract a variable from the List of Data Changes column in the REDCap 
  # logging file based on a pattern.
  #
  # Args:
  #   logging.file: Data frame with the REDCap logs.
  #   column:       New column created in the logging.file data frame where the
  #                 extracted value will be stored.
  #   pattern:      Character string containing a regular expression to be 
  #                 matched in the List of Data Changles column to extract the
  #                 desired value.
  #
  # Returns:
  #   The logging.file data frame with the new column containing the extracted
  #   value.
  logging.file[column] <- ifelse(
    test = grepl(
      pattern = pattern, 
      x       = logging.file$List.of.Data.Changes.OR.Fields.Exported
    ),
    yes  = sub(
      pattern     = pattern,
      replacement = "\\1",
      x           = logging.file$List.of.Data.Changes.OR.Fields.Exported
    ),
    no   = NA
  )
  
  logging.file
}

# Extract district from list of data changes
created.records <- ExtractFromLoggingDataChanges(
  logging.file = created.records, 
  column       = "district", 
  pattern      = kTIPTOPHHSMidlineDistrict
)
updated.records <- ExtractFromLoggingDataChanges(
  logging.file = updated.records, 
  column       = "district", 
  pattern      = kTIPTOPHHSMidlineDistrict
)

# Extract cluster from list of data changes
created.records <- ExtractFromLoggingDataChanges(
  logging.file = created.records, 
  column       = "cluster", 
  pattern      = kTIPTOPHHSMidlineCluster
)
updated.records <- ExtractFromLoggingDataChanges(
  logging.file = updated.records, 
  column       = "cluster", 
  pattern      = kTIPTOPHHSMidlineCluster
)

# Extract interviewer from list of data changes
created.records <- ExtractFromLoggingDataChanges(
  logging.file = created.records, 
  column       = "interviewer", 
  pattern      = kTIPTOPHHSMidlineInterviewer
)
updated.records <- ExtractFromLoggingDataChanges(
  logging.file = updated.records, 
  column       = "interviewer", 
  pattern      = kTIPTOPHHSMidlineInterviewer
)

# Extract interview date from list of data changes
created.records <- ExtractFromLoggingDataChanges(
  logging.file = created.records, 
  column       = "date", 
  pattern      = kTIPTOPHHSMidlineDate
)
updated.records <- ExtractFromLoggingDataChanges(
  logging.file = updated.records, 
  column       = "date", 
  pattern      = kTIPTOPHHSMidlineDate
)

# Build ordered data frame of all logs related to the creation and update actions
all.records <- rbind(created.records, updated.records)
all.records <- all.records[order(all.records$record_id, all.records$Time...Date), ]


