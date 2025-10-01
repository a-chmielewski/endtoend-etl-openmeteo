
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select day
from "analytics"."staging"."fct_city_day"
where day is null



  
  
      
    ) dbt_internal_test