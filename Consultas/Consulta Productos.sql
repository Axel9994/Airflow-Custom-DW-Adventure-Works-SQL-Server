select ProductID, p.[Name], StandardCost, ps.[Name] as Subcategory, pc.[Name] as Category
from Production.Product p
left join Production.ProductSubcategory ps
on p.ProductSubcategoryID = ps.ProductSubcategoryID
left join Production.ProductCategory pc
on ps.ProductCategoryID = pc.ProductCategoryID