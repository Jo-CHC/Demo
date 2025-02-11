USE [DemoDB]
GO
/****** Object:  UserDefinedFunction [dbo].[usf_PopulationQueryString_Get]    Script Date: 2025/2/11 週二 下午 02:01:27 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*
	Program Name: dbo.usf_PopulationQueryString_Get 

	Description: 取得 Population 的 Sql Script 
				

	Create date : 2020-11-09

	Caller(s) : 
				dbo.usf_GetPopulationComponentsQueryByMergeWay
				dbo.usp_CreatePopulationMultiComponents

	Example:
			select dbo.usf_PopulationQueryString_Get('2418,2419, 2420,2421', '1,1,2,2', 'no,union all, intersect,except', '1,2,3, 4')
			select dbo.usf_PopulationQueryString_Get('2418,2419, 2420,2421', '1,1,2,2', 'no,union all, intersect,except', '0,0,0,0')
			select dbo.usf_PopulationQueryString_Get('2418,2419, 2420,2421', '1,1,2,2', 'no,intersect, intersect,except', '0,0,0,0')

     
	Update History:
	Author			Date		Ver		Description
	------------	---------- ------	---------------------------------
	Jo Chang		2020-11-09	1.0		取得Population的Sql Script

*/

CREATE Function [dbo].[usf_PopulationQueryString_Get]
(
	@ComponentIDList VARCHAR(1000)	--	請以逗號分隔各Compoent
	, @MergeGroupList VARCHAR(200)	----	2022/04/27 Add
	----	###	@MergeWayList 範例：'no,Union All,Except'	--	第一個 no/none/empty 或空字串（因為第一個無需合併方式）
	, @MergeWayList VARCHAR(2000)		--	合併方式：（no/none/empty 表示 Component:Population = 1:1）, 組合方式的第一個使用no；第二個之後 Intersect/IN/join(交集), Union All/UA/full(聯集), Except/EX/filter (排除)
	, @MergeOrderList VARCHAR(1000)		--	若為空字串, 則優先順序依　@MergeWayList 的順序（若 Population:Component=1:N 且 MergeOrder 有相同者，則優先順序為 Union All >  Intersect > Except）
)
returns nvarchar(4000)
AS
BEGIN

	DECLARE @ComponentList TABLE (ComponentID INT, MergeGroup INT, MergeWay VARCHAR(20), MergeOrder INT, SqlQuery NVARCHAR(MAX)
					, RowNo INT, MergeOrderEqualZero_New INT, MergeOrder_Old INT)
	DECLARE @ComponentMerGroup TABLE (MergeGroup INT)

	DECLARE @tblPopulationSqlCombine TABLE(Sort INT, MergeGroup INT, MergeGroup_MergeWay VARCHAR(20), SqlScript NVARCHAR(MAX))

	
	
	/*	INSERT INTO @ComponentList	*/
	INSERT INTO @ComponentList(ComponentID, MergeGroup, MergeWay, MergeOrder, SqlQuery, MergeOrderEqualZero_New, RowNo)
	SELECT ComponentID, MergeGroup, MergeWay, MergeOrder, SqlQuery, MergeOrderEqualZero_New
	, RowNo = ROW_NUMBER() OVER(ORDER BY MergeOrderEqualZero_New, ComponentID)	--	為了當有一個MergeOrder=0時，由系統依 Intersect > Union All > Except 優先順序進行組合排序
	FROM
	(
		SELECT ComponentID=S1.col
		, MergeGroup=S2.col
		, MergeWay=S3.Col, MergeOrder=S4.Col, SqlQuery=C.SqlQuery
		, MergeOrderEqualZero_New = S3.RowNo
		FROM dbo.Components C
		CROSS APPLY dbo.utf_SplitStringIncludeRowNo(@ComponentIDList, ',') S1
		CROSS APPLY dbo.utf_SplitStringIncludeRowNo(@MergeGroupList, ',') S2
		CROSS APPLY dbo.utf_SplitStringIncludeRowNo(@MergeWayList, ',') S3
		CROSS APPLY dbo.utf_SplitStringIncludeRowNo(@MergeOrderList, ',') S4
		WHERE 1=1
		AND C.ComponentID=S1.col
		AND S2.RowNo=S1.RowNo AND S3.RowNo=S1.RowNo AND S4.RowNo=S1.RowNo
	) c
	
	Update @ComponentList SET MergeOrder_Old = MergeOrder


	IF (select count(*) from @ComponentList where MergeOrder=0) > 0
	BEGIN

		--	為了當有一個MergeOrder=0時，由系統依 Intersect > Union All > Except 優先順序進行組合排序
		UPDATE @ComponentList SET MergeOrder=RowNo
		
	END	--	IF (select count(*) from @ComponentList where MergeOrder=0) > 0


	----PRINT '取得不重複的 MergeGroup'
	INSERT INTO @ComponentMerGroup (MergeGroup)
	SELECT DISTINCT MergeGroup FROM @ComponentList



	DECLARE @MergeGroupRowIndex INT = 1
	DECLARE @MergeGroupCount INT = (SELECT COUNT(*) FROM @ComponentMerGroup)
			, @MergeGroup INT
			, @MergeGroupPre INT = 0
			, @MergeGroup_MergeWay VARCHAR(20) = ''

	DECLARE @SQL NVARCHAR(MAX) = ''
			, @SQL_ByMerGroup NVARCHAR(MAX) = ''
			, @SQL_Merge VARCHAR(20) = ''

	/*	MergeGroup 組合群組迴圈（主要用於有交集、聯集、排除時，需要或群組順序時，如：(A交集B交集C) 排除 (B交集C交集D)） --- START ---	*/
	WHILE @MergeGroupCount > 0
	BEGIN
		SELECT TOP 1 @MergeGroup=MergeGroup FROM @ComponentMerGroup ORDER BY MergeGroup

		IF @MergeGroupRowIndex = 1
		BEGIN
			SET @MergeGroup_MergeWay = 'no'
		END
		ELSE
		BEGIN
			SET @MergeGroup_MergeWay = (SELECT TOP 1 MergeWay FROM @ComponentList WHERE MergeGroup=@MergeGroup ORDER BY MergeOrder)
		END


		DECLARE @RowCount INT = (select count(*) from @ComponentList WHERE MergeGroup=@MergeGroup)
		
	
		DECLARE @RowIndex INT = 1
		DECLARE @ComponentID INT
				----, @Include BIT	---- 2022/04/25 Mark : 因為使用 MergeOrder 後 [Include] 沒有用處了
				, @MergeWay VARCHAR(20)
				, @MergeOrder INT
				, @SqlQuery NVARCHAR(MAX)
		DECLARE @MergeWay_Pre VARCHAR(20) = 'no'	---- 第一個為no
				, @MergeGroup_Pre INT = 0	---- 2022/04/27 Add : @MergeGroup_Pre
				, @MergeWay_FirstPerGroup VARCHAR(20) = ''	----  每個@MergeGroup 的第一個Componet的MergeWay 2022/04/27 Add : @MergeWa_FirstPerGroup
	
		SET @SQL = ''
		
		/*	依 MergeGroup 取得組合指令 --- START ---	*/
		While @RowCount > 0
		Begin

			SELECT TOP 1 @ComponentID=ComponentID, @MergeWay=MergeWay, @MergeOrder=MergeOrder, @SqlQuery=SqlQuery 
				FROM @ComponentList WHERE MergeGroup=@MergeGroup ORDER BY MergeOrder

			if (@RowIndex = 1)
			begin
				SET @Sql = @SqlQuery
			end	----	if (@RowIndex = 1)
			else
			begin
				
				/*	取得組合方式：Intersect/IN/join(交集), Union All/UA/full(聯集), Except/EX/filter --- START ---		*/
				if (@MergeWay in ('intersect', 'join', 'in'))
				begin
					----PRINT '交集'
					set @SQL_Merge = ' intersect '
				end	--	if (@MergeWay in ('', 'intersect', 'join', 'in'))
				else if (@MergeWay in ('Union All', 'union', 'UA', 'full'))
				begin
					----PRINT '聯集'
					set @SQL_Merge = ' union '	---- 2022/08/25 Modify : 聯集部份，將 Union all 改成 Union
				end	--	else if (@MergeWay in ('Union All', 'UA', 'full'))
				else if (@MergeWay in ('Except', 'EX', 'filter'))
				begin
					----PRINT '排除'
					set @SQL_Merge = ' Except '
				end	--	else if (@MergeWay in ('Except', 'EX', 'filter'))


				----	若上一個組合方式與這次不同，要加(), 因為組合方式順序會影響數量
				if (@RowIndex >= 3) and (@MergeWay_Pre <> @MergeWay)
				begin
					------PRINT '有3個以上的Component 且組合方式有變 (intersect -> intersect -> except 或 intersect -> union all -> intersect 或 intersect -> union all -> Except 或 ......)'
					set @SQL = 'select UID from (' + @SQL + ') popGroup' + Cast(@MergeGroup as varchar) + '_' + CAST(@RowIndex AS VARCHAR) + ' ' + @MergeWay + ' ' + @SqlQuery
				end	--	if (@MergeWay_Pre <> @MergeWay)
				else
				begin
					------PRINT '有2個以上的Component, 組合方式不變（如：皆是intersect, 或皆是 union all....）'
					set @SQL = @SQL + ' ' + @MergeWay + ' ' + @SqlQuery
				end

			end	----	if (@RowIndex = 1)



			DELETE @ComponentList WHERE ComponentID=@ComponentID AND MergeOrder=@MergeOrder
			SET @RowCount = (select count(*) from @ComponentList WHERE MergeGroup=@MergeGroup)

			SET @RowIndex = @RowIndex + 1
			SET @MergeWay_Pre = @MergeWay

		End	----	While @RowCount > 0
		/*	依 MergeGroup 取得組合指令 --- END ---	*/


		INSERT INTO @tblPopulationSqlCombine(Sort, MergeGroup, MergeGroup_MergeWay, SqlScript)
		SELECT @MergeGroupRowIndex, @MergeGroup, @MergeGroup_MergeWay, @SQL


		SET @MergeGroupPre=@MergeGroup

		DELETE @ComponentMerGroup WHERE MergeGroup=@MergeGroup
		SET @MergeGroupCount = (select count(*) from @ComponentMerGroup)
		
		SET @MergeGroupRowIndex = @MergeGroupRowIndex + 1
		

	END	----	WHILE @MergeGroupCount > 0
	/*	MergeGroup 組合群組迴圈（主要用於有交集、聯集、排除時，需要或群組順序時，如：(A交集B交集C) 排除 (B交集C交集D)） --- END ---	*/



	IF (SELECT COUNT(*) FROM @tblPopulationSqlCombine) = 1
	BEGIN
		SET @SQL = (SELECT SqlScript FROM @tblPopulationSqlCombine)
	END
	ELSE
	BEGIN

		SET @SQL = (SELECT ' ' + MergeGroup_MergeWay + ' select uid from (' + SqlScript + ') popG' + CAST(MergeGroup AS VARCHAR) FROM @tblPopulationSqlCombine FOR XML PATH(''))
		SET @SQL = REPLACE(@SQL, ' no', 'select uid from (') + ') pop'
		SET @SQL = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@SQL, '&lt;', '<'), '&gt;', '>'), '&amp;', '&'), '&apos;', ''''), '&quot;', '"')
	END
	
	Return @SQL

END


