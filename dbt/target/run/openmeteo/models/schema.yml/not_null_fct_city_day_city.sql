
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select city
from "analytics"."staging"."fct_city_day"
where city is null



  
  
      
    ) dbt_internal_test