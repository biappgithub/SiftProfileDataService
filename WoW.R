###########################################################################################
# NAME: PP Rocket Package.R
# DESC: A rule-based NBO model for prepaid users. 
#
# LOG (mm/dd/yyyy) 
#      04/18/2016 TK - Create initial version
#
###########################################################################################

user_status <- "TOPPING"
m_segment_type <- "LV"
print("=== Start Check Param ===")
library('org.renjin.cran:exptest')
print(AGE)
print(USER_STATUS)
print(CREDIT_SERIES)
print("=== End Check Param ===")
#
#------------------------------------------------------------------------------------------
# (10) Model outputs for event payload
#------------------------------------------------------------------------------------------
user_status
m_segment_type
