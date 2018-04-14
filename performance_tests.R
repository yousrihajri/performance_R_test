
library(tidyverse)
options(scipen = 999, digits = 2)


#### DATA PREPARATION ####
##########################

data_08_2017 <- readit("https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-08.csv")
data_09_2017 <- readit("https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-09.csv")
data_10_2017 <- readit("https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-10.csv")
data_11_2017 <- readit("https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-11.csv")
data_12_2017 <- readit("https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-12.csv")
data_brut <- rbind(data_08_2017, data_09_2017, data_10_2017, data_11_2017, data_12_2017) 
remove(data_08_2017, data_09_2017, data_10_2017, data_11_2017, data_12_2017)

#Prepare XDF

colClasses <- c('VendorID' = "factor",
                'tpep_pickup_datetime' = "POSIXct",
                'tpep_dropoff_datetime' = "POSIXct",
                'passenger_count' = "integer",
                'trip_distance' = "numeric",
                'RatecodeID' = "factor",
                'store_and_fwd_flag' = "factor",
                'PULocationID' = "numeric",
                'DOLocationID' = "numeric",
                'payment_type' = "factor",
                'fare_amount' = "numeric",
                'extra' = "numeric",
                'mta_tax' = "numeric",
                'tip_amount' = "numeric",
                'tolls_amount' = "numeric",
                'improvement_surcharge' = "numeric",
                'total_amount' = "numeric")

##Create xdf connections
rxImport(inData=data_brut,
         outFile="data_brut_xdf.xdf",
         overwrite = TRUE,
         colClasses =  colClasses,
         append = "none")

data_brut_xdf_connection <- RxXdfData("data_brut_xdf.xdf")

rxImport(inData=data_brut[1:5,],
         outFile="sample_xdf.xdf",
         overwrite = TRUE,
         colClasses =  colClasses,
         append = "none")


sample_xdf_connection <- RxXdfData("sample_xdf.xdf")

#### PREPARING THE LOOP ####
############################



base_R_time <- as.vector(0)
tidyverse_time <- as.vector(0)
pipe_base_R_time <- as.vector(0)
xdf_time <- as.vector(0)
correl_time_speed <- as.data.frame(0)
correl_time_speed_revo <- as.vector(0)
correl_time_speed_base <- as.vector(0)
correl_time_speed_tidyverse <- as.vector(0)
df <- as.data.frame(0)


#Take data by a million row addition at a time in a for loop
for (i in 1:45) {
  
  print(paste("iteration n°",i))
  print(paste("number of rows ",i*1000000))

  
#Using RevoScaleR
print("RevoScaleR")
start_time <- Sys.time()
rxDataStep(inData = data_brut_xdf_connection,
             outFile = sample_xdf_connection,
             overwrite = TRUE,
             transforms = list(trip_time=tpep_dropoff_datetime-tpep_pickup_datetime,
                              trip_speed=trip_distance/((as.numeric(trip_time))/3600)),
             rowSelection = (trip_distance<=50 & trip_distance>=1 &
                             trip_time<=5000 & trip_time>=1 &
                             trip_speed<=100 & trip_speed>=1),
             varsToKeep = c("VendorID","trip_distance"),
             numRows = i*1000000)
correl <- rxCor(formula = ~trip_time+trip_speed, data = sample_xdf_connection)
correl_time_speed_revo <- c(correl_time_speed_revo, correl[1,2])
end_time <- Sys.time()
xdf_time [[i]] <- end_time-start_time
  
  
#Using step_by_step Base R
print("step_by_step Base R")
start_time <- Sys.time()
data <- data_brut[1:(i*1000000),]
data <- subset (data, select = c("VendorID","tpep_pickup_datetime","tpep_dropoff_datetime","trip_distance")) 
data <- subset (data, trip_distance<=50 & trip_distance>=1)
data <- within (data, {trip_time=tpep_dropoff_datetime-tpep_pickup_datetime})
data$trip_time <- as.numeric(data$trip_time)
data <- subset (data, trip_time<=5000 & trip_time>=1)
data <- data[,-c(2,3)]
data <- within (data, {trip_speed=trip_distance/(trip_time/3600)})
data <- subset (data, trip_speed<=100 & trip_speed>=1)
correl <- cor(subset(data, select= c("trip_time", "trip_speed")),
                         use = "pairwise.complete.obs",
                         method = "pearson")
correl_time_speed_base <- c(correl_time_speed_base, correl[1,2])
end_time <- Sys.time()
base_R_time [[i]] <- end_time-start_time

#Using Tidyverse pipe
print("Tidyverse pipe")
start_time <- Sys.time()
data <- data_brut[1:(i*1000000),]
data <- data %>%
  select("VendorID","tpep_pickup_datetime","tpep_dropoff_datetime","trip_distance") %>%
  filter(trip_distance<=50 & trip_distance>=1) %>%
  mutate(trip_time=as.numeric(tpep_dropoff_datetime-tpep_pickup_datetime)) %>%
  filter(trip_time<=5000 & trip_time>=1) %>%
  select(-c(2,3)) %>%
  mutate(trip_speed=trip_distance/(trip_time/3600)) %>%
  filter(trip_speed<=100 & trip_speed>=1) %>%
  select(trip_time, trip_speed) %>%
  cor(use = "pairwise.complete.obs",
      method = "pearson")
correl_time_speed_tidyverse <- c(correl_time_speed_tidyverse, data[1,2])
  
end_time <- Sys.time()
tidyverse_time [[i]] <- end_time-start_time

#Return df

df <- rbind(df, i*1000000)
correl_time_speed <- rbind(correl_time_speed, i*1000000)

}

#Correlations
correl_time_speed <- cbind (correl_time_speed, 
                            correl_time_speed_base,
                            correl_time_speed_tidyverse,
                            correl_time_speed_revo) %>%
  rename(V1 = '0')
correl_time_speed <- correl_time_speed [-1,] %>% as.data.frame()
correl_time_speed <- gather(correl_time_speed, select=c("correl_time_speed_base", "correl_time_speed_tidyverse", "correl_time_speed_revo")) 
correl_time_speed$key <- as.factor(correl_time_speed$key)
correl_time_speed$value <- as.numeric(correl_time_speed$value*100)

plot1 <- ggplot (data = correl_time_speed) +
  geom_point (aes(x = V1, y=value, color=key)) +
  geom_smooth(aes(x = V1, y=value, color=key), method = "loess", alpha=0.3) +
  coord_cartesian(xlim=c(0,max(correl_time_speed$V1)), ylim = c(0,max(correl_time_speed$value))) +
  theme_classic()

#Plotting the result All_time

base_R_time <- cbind(df[-1,], base_R_time)
All_time <- cbind(base_R_time, tidyverse_time, xdf_time) %>% as.data.frame()
All_time <- gather (data = All_time, select= c("base_R_time", "tidyverse_time", "xdf_time"))
All_time$key <- as.factor(All_time$key)


plot2 <- ggplot(data = All_time) +
  geom_point (aes(x = V1, y=value, color=key)) +
  geom_smooth(aes(x = V1, y=value, color=key), method = "loess", alpha=0.3) +
  coord_cartesian(xlim=c(0,max(All_time$V1))) +
  theme_classic()


remove(base_R_time, df, end_time, start_time, tidyverse_time, xdf_time,
       correl, correl_time_speed_base, correl_time_speed_revo, correl_time_speed_tidyverse, data)

plot1
plot2
