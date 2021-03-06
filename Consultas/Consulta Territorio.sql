create view vTerritorios as
select ROW_NUMBER() over(ORDER BY datos.City, datos.StateProvinceID, datos.State_Name, datos.Territory_Name) as CityID,
       datos.City,
	   datos.StateProvinceID,
	   datos.State_Name,
	   datos.Territory_Name
from (SELECT distinct 
                ad.[City],
                ad.[StateProvinceID],
				stp.Name as State_Name,
				slt.Name as Territory_Name
  FROM [AdventureWorks2019].[Person].[Address] ad
  left join Person.StateProvince stp
  on ad.StateProvinceID = stp.StateProvinceID
  left join Sales.SalesTerritory slt
  on stp.TerritoryID = slt.TerritoryID) datos