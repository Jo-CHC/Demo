
/*

	Program Name : job.usp_MashUpPopulationUIDList_Update
				 
	Description : 更新混搭型 Popultion 名單
	Create date : 2022-11-01
   
	Caller(s) : 
				Job - 【Mobi_CDP_UpdateMashUpPopulationUIDList】
				 
	Example : 
				EXEC job.usp_MashUpPopulationUIDList_Update

     
	Update History:
	Author			Date		Ver		Description
	-------	------	----		-----------------
	Jo Chang		2022-11-01	V1.0	更新混搭型 Popultion 名單


*/

CREATE PROCEDURE [job].[usp_UpdateMashUpPopulationUIDList]
AS
BEGIN

	/* 處理使用外部組件【MobiStringSplit】的函數【dbo.utf_clr_SplitStringIncludeRowNo】安全性問題（開啟） */
	ALTER DATABASE [我的資料庫] SET TRUSTWORTHY ON



	IF OBJECT_ID('tempdb..#tmpImport_MashUpPopulation') IS NOT NULL DROP TABLE #tmpImport_MashUpPopulation
	IF OBJECT_ID('tempdb..#tmpImport_MashUpPopulation2While') IS NOT NULL DROP TABLE #tmpImport_MashUpPopulation2While
	IF OBJECT_ID('tempdb..#tmpUIDList') IS NOT NULL DROP TABLE #tmpUIDList
	IF OBJECT_ID('tempdb..#tmpUIDList_Pre') IS NOT NULL DROP TABLE #tmpUIDList_Pre
	IF OBJECT_ID('tempdb..#tmpMashUpPopuList') IS NOT NULL DROP TABLE #tmpMashUpPopuList
	IF OBJECT_ID('tempdb..#tmpPopulationUpdateList') IS NOT NULL DROP TABLE #tmpPopulationUpdateList
	IF OBJECT_ID('tempdb..#tmpToSendAlertEmail') IS NOT NULL DROP TABLE #tmpToSendAlertEmail


	CREATE TABLE #tmpImport_MashUpPopulation (SourceTable VARCHAR(100), SourceID BIGINT, ExecuteSEQ VARCHAR(50), ImportDate DATE
			, PopulationID INT, UIDList VARCHAR(MAX), ListSize INT, MashUpPopuIDs VARCHAR(2000) DEFAULT('')
			, MashUpPopuIDsSize VARCHAR(2000) DEFAULT(''), MashUpPopuIDsProportion VARCHAR(2000) DEFAULT('')
			, MashUp_PopulationSettingID INT DEFAULT(0)
			, DestDBName VARCHAR(100) DEFAULT(''), DestTableName VARCHAR(200) DEFAULT('')
			, DestDBNameHistBackup VARCHAR(100) DEFAULT(''), DestTableNameHistBackup VARCHAR(200) DEFAULT('')
			, AdvertiserID INT DEFAULT(0), CampaignID INT DEFAULT(0)
			, AlgorithmGroupID INT DEFAULT(0)
			, GroupID INT DEFAULT(0), SubGroupID INT DEFAULT(0)
			, SQLScript_Insert NVARCHAR(MAX) DEFAULT(''))
	CREATE TABLE #tmpUIDList (PopulationID INT, UID INT, RejectedStatus INT DEFAULT(0), Prefix5 VARCHAR(5) DEFAULT(''), NCCReleaseStatus INT DEFAULT(0))
	CREATE TABLE #tmpUIDList_Pre (PopulationID INT, UID INT, RejectedStatus INT DEFAULT(0), InPopulationRawDataStatus INT DEFAULT(0))
	CREATE TABLE #tmpMashUpPopuList(RowNo INT DEFAULT(0), PopulationID INT DEFAULT(0), PopulationName NVARCHAR(500) DEFAULT(''), PopulationSize INT DEFAULT(0)
			, PopulationProportion VARCHAR(38) DEFAULT(''), ProportionPercentage VARCHAR(20) DEFAULT('')
			, Sort INT)
	CREATE TABLE #tmpPopulationUpdateList (PopulationID INT, PopulationName NVARCHAR(500) DEFAULT(''), UpdateDate DATETIME
			, ListSizeOri INT, ListSize_Population INT DEFAULT(0), ListSize_Rejected INT DEFAULT(0)
			, ListSize_Ori_Pre INT, ListSize_Population_Pre INT DEFAULT(0), ListSize_Rejected_Pre INT
			, ListSize_Overlap INT DEFAULT(0), ListSize_Population_Overlap INT DEFAULT(0)
			, OerlapRate_Ori FLOAT DEFAULT(0), OerlapRate_Population FLOAT DEFAULT(0)
			, MashUpPopuIDs VARCHAR(2000) DEFAULT(''), MashUpPopuNameList NVARCHAR(MAX) DEFAULT('')
			, MashUpPopuIDsSize VARCHAR(2000) DEFAULT(''), MashUpPopuIDsProportion VARCHAR(2000) DEFAULT(''), MashUpPopuIDsPercentage VARCHAR(2000) DEFAULT('')
			, MashUpPopuIDsPopulationSize VARCHAR(2000) DEFAULT(''))
			
			
	
	DECLARE @SPName varchar(100) = (select dbo.usf_GetStoredProcedureWholeName(@@PROCID))
	

	DECLARE @PopulationTableLastID BIGINT = 0
		
	BEGIN TRY

		PRINT 'INSERT INTO #tmpImport_MashUpPopulation --- START --- ' + CONVERT(VARCHAR(20), GETDATE(), 120)
		INSERT INTO #tmpImport_MashUpPopulation(SourceTable, SourceID, ExecuteSEQ, ImportDate, PopulationID, UIDList, ListSize, MashUpPopuIDs, MashUpPopuIDsSize)
		SELECT SourceTable='我的資料資料表', SourceID=ID
			, ExecuteSEQ, ImportDate, PopulationID, UIDList, ListSize, MashUpPopuIDs, MashUpPopuIDsSize
		FROM betterDynamicTemp.uvw_import_MashUpPopulation
		WHERE 1=1
		AND Status = 0
		AND PopulationID > 0


		IF EXISTS (SELECT * FROM #tmpImport_MashUpPopulation)
		BEGIN
			
			PRINT 'UPDATE #tmpImport_MashUpPopulation 取得 Population 名單資料表: DestTableName, SQLScript_Insert --- START --- ' + CONVERT(VARCHAR(20), GETDATE(), 120)
			UPDATE ta SET MashUp_PopulationSettingID=ref.ID
				, DestDBName=ref.DestDBName, DestTableName=ref.DestTableName
				, DestDBNameHistBackup=ref.DestDBNameHistBackup, DestTableNameHistBackup=ref.DestTableNameHistBackup
				, AdvertiserID=ref.AdvertiserID, CampaignID=ref.CampaignID
				, AlgorithmGroupID=ref.AlgorithmGroupID
				, GroupID=ref.GroupID
				, SQLScript_Insert=ref.SQLScript_Insert
			FROM #tmpImport_MashUpPopulation ta
			INNER JOIN (
				SELECT *
				FROM dbo.MashUp_PopulationSetting
				WHERE IsActive=1
			) ref ON ref.PopulationID=ta.PopulationID
			PRINT 'UPDATE #tmpImport_MashUpPopulation 取得 Population 名單資料表: DestTableName, SQLScript_Insert --- END --- ' + CONVERT(VARCHAR(20), GETDATE(), 120) + CHAR(10) + CHAR(10)



			SELECT * INTO #tmpImport_MashUpPopulation2While FROM #tmpImport_MashUpPopulation
			
			DECLARE @SQL NVARCHAR(MAX) = ''
					, @SQL_Where VARCHAR(1000) = ''
			DECLARE @DestTableNameHistBackupExistsStatus INT = 0
					, @DestTableOnlyNameHistBackup VARCHAR(100)
					, @DestTableFullNameHistBackup VARCHAR(200)
					, @DestTableNameAndSchema VARCHAR(100)
					, @TableExists BIT
			DECLARE @ListSize_Count INT = -1
					, @ListSize_Population INT = -1
					, @ListSize_Rejected INT = -1
					, @ListSize_Overlap INT = 0	-- 與上一次的重疊數
					, @ListSize_Population_Overlap INT = 0	-- 與上一次的重疊數
					, @ListSize_NCCNotRelease INT = 0	-- NCC 未釋出
					, @import_MashUpPopulationID_Pre INT = 0	-- 上一次的
					, @UID_List_Pre VARCHAR(MAX) = ''	-- 上一次的
					, @ListSize_Ori_Pre INT = 0	-- 上一次的
					, @ListSize_Population_Pre INT = 0	-- 上一次的
					, @ListSize_Rejected_Pre INT = 0	-- 上一次的

					
			DECLARE @ExecuteDateFrom DATETIME
					, @ExecuteDateTo DATETIME
					, @ExecuteSeconds INT = -1

			DECLARE @TotalPopulation INT = (SELECT COUNT(*) FROM #tmpImport_MashUpPopulation2While)
			DECLARE @ImportDate DATE
					, @SourceTable VARCHAR(100)
					, @SourceID INT
					, @MashUp_PopulationSettingID INT
					, @PopulationID INT
					, @UIDList VARCHAR(MAX)
					, @ListSize INT
					, @MashUpPopuIDs VARCHAR(2000) = ''	
					, @MashUpPopuIDsSize VARCHAR(2000) = ''	
					, @DestDBName VARCHAR(100)
					, @DestTableName VARCHAR(100)
					, @DestDBNameHistBackup VARCHAR(100)
					, @DestTableNameHistBackup VARCHAR(100)
					, @AdvertiserID INT
					, @CampaignID INT
					, @GroupID INT
					, @AlgorithmGroupID INT	
			DECLARE @MashUpPopuNameList NVARCHAR(MAX) = ''
					, @MashUpPopuIDCount INT = 0	-- 混搭了幾個 Population
					, @MashUpPopuIDsProportion VARCHAR(2000) = ''	
					, @MashUpPopuIDsPercentage VARCHAR(2000) = ''	

			While (@TotalPopulation > 0)
			Begin
				
				DELETE #tmpUIDList
				DELETE #tmpUIDList_Pre
				DELETE #tmpMashUpPopuList


				SELECT TOP 1 @ImportDate=ImportDate, @SourceTable=SourceTable, @SourceID=SourceID
					, @MashUp_PopulationSettingID=MashUp_PopulationSettingID
					, @PopulationID=PopulationID, @UIDList=UIDList, @ListSize=ListSize
					, @MashUpPopuIDs=MashUpPopuIDs, @MashUpPopuIDsSize=MashUpPopuIDsSize
					, @AdvertiserID=AdvertiserID, @CampaignID=CampaignID
					, @AlgorithmGroupID=AlgorithmGroupID
					, @GroupID=GroupID
					, @DestDBName=DestDBName, @DestTableName=DestTableName
					, @DestDBNameHistBackup=DestDBNameHistBackup, @DestTableNameHistBackup=DestTableNameHistBackup
					, @DestTableFullNameHistBackup=@DestDBNameHistBackup + '.' + @DestTableNameHistBackup
					, @DestTableOnlyNameHistBackup=REPLACE(@DestTableNameHistBackup, 'pop.', '')
				FROM #tmpImport_MashUpPopulation2While
				ORDER BY SourceID

				

				PRINT '※※※ 處理 PopulationID = ' + CAST(@PopulationID AS VARCHAR) + ' --- START --- ' + CONVERT(VARCHAR(20), GETDATE(), 120)
				
				SET @ExecuteDateFrom = GETDATE()


				/*	檢查舊資料備份資料表是否存在（DB=我的資料庫Hist）	*/
				DECLARE @TableHistExists BIT = (SELECT dbo.usf_CheckTableHistExists(@DestTableNameHistBackup))
				If (@TableHistExists=0)
				Begin
					PRINT CHAR(9) + CHAR(9) + 'EXEC dbo.usp_CreateHistMashUpPopulationTable : 檢查備份資料表是否存在，不存在則建立資料表' + CONVERT(VARCHAR(20), GETDATE(), 120) + CHAR(10)
					EXEC dbo.usp_CreateHistMashUpPopulationTable @DBName='我的資料庫Hist', @TableNameNoSchema=@DestTableOnlyNameHistBackup
				End


				/*	與得這個 Population 的 WHERE 條件	*/
				SET @SQL_Where = 'WHERE 1=1' + CHAR(10)
				SET @SQL_Where = @SQL_Where + ' AND AdvertiserID=' + CAST(@AdvertiserID AS VARCHAR) + CHAR(10)
				SET @SQL_Where = @SQL_Where + ' AND CampaignID=' + CAST(@CampaignID AS VARCHAR) + CHAR(10)
				SET @SQL_Where = @SQL_Where + ' AND GroupID=' + CAST(@GroupID AS VARCHAR) + CHAR(10)
				SET @SQL_Where = @SQL_Where + ' AND AlgorithmGroupID=' + CAST(@AlgorithmGroupID AS VARCHAR) + CHAR(10)


				PRINT CHAR(9) + 'INSERT INTO #tmpUIDList(PopulationID : 取得名單明細、Rejected 狀況 --- START --- ' + CONVERT(VARCHAR(20), GETDATE(), 120)
				
				INSERT INTO #tmpUIDList(PopulationID, UID)
				SELECT @PopulationID, ValueString
				FROM dbo.utf_clr_SplitStringIncludeRowNo (@UIDList, ',')

				
				PRINT 'UPDATE #tmpUIDList : RejectedStatus'
				UPDATE ta SET RejectedStatus = 1
				FROM #tmpUIDList ta
				INNER JOIN msisdnPop.uvw_RejectedListRecently ref ON ref.UID = ta.UID


				PRINT 'UPDATE #tmpUIDList : Prefix5'
				UPDATE ta SET Prefix5 = ref.Prefix5
				FROM #tmpUIDList ta
				INNER JOIN msisdnPop.uvw_Msisdns ref ON ref.UID = ta.UID


				PRINT 'UPDATE #tmpUIDList : NCCReleaseStatus'
				UPDATE ta SET NCCReleaseStatus = (CASE WHEN ref.IsActive = 1 THEN 1 ELSE -1 END)
				FROM #tmpUIDList ta
				INNER JOIN msisdnPop.uvw_NCCReleasePrefix ref ON ref.Prefix5 = ta.Prefix5

				SET @ListSize_Count = (SELECT COUNT(*) FROM #tmpUIDList)
				SET @ListSize_Population = (SELECT COUNT(*) FROM #tmpUIDList WHERE RejectedStatus = 0)
				SET @ListSize_Rejected = (SELECT COUNT(*) FROM #tmpUIDList WHERE RejectedStatus = 1)
				SET @ListSize_NCCNotRelease = (SELECT COUNT(*) FROM #tmpUIDList WHERE NCCReleaseStatus = 0)

				PRINT CHAR(9) + 'INSERT INTO #tmpUIDList(PopulationID : 取得名單明細、Rejected 狀況 --- END --- ' + CONVERT(VARCHAR(20), GETDATE(), 120) + CHAR(10)



				PRINT CHAR(9) + '取得上一次的 Population 資料，為了計算重疊數 :  --- START --- ' + CONVERT(VARCHAR(20), GETDATE(), 120)
				SELECT TOP 1 @import_MashUpPopulationID_Pre=ID, @UID_List_Pre=UIDList
				FROM betterDynamicTemp.uvw_import_MashUpPopulation 
				WHERE 1=1
				AND Status=10
				AND PopulationID = @PopulationID
				ORDER BY ID DESC


				SELECT @ListSize_Ori_Pre=ListSize_Count, @ListSize_Population_Pre=ListSize_Population, @ListSize_Rejected_Pre=ListSize_Rejected
				FROM logs.MashUp_PopulationUpdateLogs
				WHERE 1=1
				AND SourceTable = '我的資料表'
				AND SourceID = @import_MashUpPopulationID_Pre

				
				
				If ISNULL(@UID_List_Pre, '') <> ''
				Begin
					INSERT INTO #tmpUIDList_Pre(PopulationID, UID)
					SELECT @PopulationID, ValueString
					FROM dbo.utf_clr_SplitStringIncludeRowNo (@UID_List_Pre, ',')


					SET @SQL = 'UPDATE ta SET InPopulationRawDataStatus=1' + CHAR(10)
							 + 'FROM #tmpUIDList_Pre ta' + CHAR(10)
							 + 'INNER JOIN (' + CHAR(10)
							 + ' SELECT * FROM ' + @DestTableName + CHAR(10)
							 + @SQL_Where + CHAR(10)
							 + ') ref ON ref.UID=ta.UID' + CHAR(10)
					PRINT CHAR(9) + CHAR(9) + '@SQL (UPDATE tmpUIDList_Pre) = ' + CHAR(10) + CHAR(9) + CHAR(9) + @SQL
					EXEC (@SQL)
				
					SET @ListSize_Overlap = (SELECT COUNT(DISTINCT UID) FROM (SELECT UID FROM #tmpUIDList INTERSECT SELECT UID FROM #tmpUIDList_Pre) A)
					SET @ListSize_Population_Overlap = (SELECT COUNT(DISTINCT UID) FROM (SELECT UID FROM #tmpUIDList WHERE RejectedStatus=0 INTERSECT SELECT UID FROM #tmpUIDList_Pre WHERE InPopulationRawDataStatus=1) A)
				End	--	If ISNULL(@UID_List_Pre, '') <> ''
				Else
				Begin
					SET @ListSize_Overlap = 0
					SET @ListSize_Population_Overlap = 0
				End	--	Else	If ISNULL(@UID_List_Pre, '') <> ''
				
				PRINT CHAR(9) + '取得上一次的 Population 資料，為了計算重疊數 --- END --- ' + CONVERT(VARCHAR(20), GETDATE(), 120) + CHAR(10)
				



				PRINT CHAR(9) + 'INSERT INTO 【' + @DestTableFullNameHistBackup + '】 : 刪除舊名單前，先備份上一次混搭的名單 --- START --- ' + CONVERT(VARCHAR(20), GETDATE(), 120)

				SET @SQL = 'INSERT INTO ' + @DestTableFullNameHistBackup + '(SourceID, ImportDate, AdvertiserID, CampaignID' + CHAR(10)
						  + ', Uid, AlgorithmGroupID, GroupID, IsActive, Status, Note, InsertDatetime, InsertUser, UpdateDatetime, UpdateUser, TranUser)' + CHAR(10)
						  + 'SELECT ID, ImportDate, AdvertiserID, CampaignID, Uid, AlgorithmGroupID, GroupID, IsActive, Status, Note, InsertDatetime, InsertUser, UpdateDatetime, UpdateUser' + CHAR(10)
						  + ', ''' + @SPName + '''' + CHAR(10)
						  + 'FROM ' + @DestTableName + CHAR(10)
						  + @SQL_Where
				PRINT CHAR(9) + CHAR(9) + '@SQL (INSERT INTO ' +  @DestTableFullNameHistBackup+ ') = ' + CHAR(10) + CHAR(9) + CHAR(9) + @SQL
				EXEC (@SQL)

				PRINT CHAR(9) + 'INSERT INTO 【' + @DestTableFullNameHistBackup + '】 : 刪除舊名單前，先備份上一次混搭的名單 --- END --- ' + CONVERT(VARCHAR(20), GETDATE(), 120) + CHAR(10)





				PRINT CHAR(9) + 'DELETE 【' + @DestTableName + '】 : 刪除上一次混搭的名單 --- START --- ' + CONVERT(VARCHAR(20), GETDATE(), 120)
				
				SET @SQL = 'DELETE ' + @DestTableName + ' ' + @SQL_Where
				PRINT CHAR(9) + CHAR(9) + '@SQL (DELETE ' +  @DestTableName+ ') = ' + CHAR(10) + CHAR(9) + CHAR(9) + @SQL
				EXEC (@SQL)
				
				PRINT CHAR(9) + 'DELETE 【' + @DestTableName + '】 : 刪除上一次混搭的名單 --- END --- ' + CONVERT(VARCHAR(20), GETDATE(), 120) + CHAR(10)



				PRINT CHAR(9) + 'INSERT INTO 【' + @DestTableName + '】 : 寫入新混搭的名單清單 --- START --- ' + CONVERT(VARCHAR(20), GETDATE(), 120)
				
				SET @SQL = 'INSERT INTO ' + @DestTableName + ' (ImportDate, AdvertiserID, CampaignID, Uid, AlgorithmGroupID, GroupID, InsertUser, UpdateUser)' + CHAR(10)
						 + 'SELECT ''' + CONVERT(VARCHAR(10), @ImportDate, 111) + '''' + CHAR(10)
						 + ', ' + CAST(@AdvertiserID AS VARCHAR) + ', ' + CAST(@CampaignID AS VARCHAR) + CHAR(10)
						 + ', UID, ' + CAST(@AlgorithmGroupID AS VARCHAR) + ', ' + CAST(@GroupID AS VARCHAR) + CHAR(10)
						 + ', ''' + @SPName + ''',''' + @SPName + ''''  + CHAR(10)
						 + 'FROM #tmpUIDList' + CHAR(10)
						 + 'WHERE 1=1' + CHAR(10)
						 + 'AND (RejectedStatus = 0 OR NCCReleaseStatus = 1)' + CHAR(10)
				PRINT CHAR(9) + CHAR(9) + '@SQL (INSERT INTO ' +  @DestTableName+ ') = ' + CHAR(10) + CHAR(9) + CHAR(9) + @SQL
				EXEC (@SQL)

				PRINT CHAR(9) + 'INSERT INTO 【' + @DestTableName + '】 : 寫入新混搭的名單清單 --- END --- ' + CONVERT(VARCHAR(20), GETDATE(), 120) + CHAR(10)
				


				PRINT CHAR(9) + 'UPDATE betterDynamicTemp.uvw_import_MashUpPopulation : Status 更新已處理的新混搭Population --- START --- ' + CONVERT(VARCHAR(20), GETDATE(), 120)
				UPDATE ta SET Status=10, UpdateDatetime=GETDATE(), UpdateUser=@SPName, UpdateSP=@SPName
				FROM betterDynamicTemp.uvw_import_MashUpPopulation ta
				WHERE ID = @SourceID
				PRINT CHAR(9) + 'UPDATE betterDynamicTemp.uvw_import_MashUpPopulation : Status 更新已處理的新混搭Population --- END --- ' + CONVERT(VARCHAR(20), GETDATE(), 120) + CHAR(10)
				


				PRINT CHAR(9) + '計算Population Size（僅Population本身） --- ' + CONVERT(VARCHAR(20), GETDATE(), 120)
				EXEC TWGenList.Coll.usp_CalculatePopulationSizeOnly @PopID=@PopulationID, @ActiveStatus=1, @CalculateType=1, @QuantityStatusMoreThan = 1, @IsUpdateFETServer=1
				


				PRINT 'INSERT INTO #tmpMashUpPopuList : 取得混搭哪些 Population --- ' + CONVERT(VARCHAR(20), GETDATE(), 120)
				INSERT INTO #tmpMashUpPopuList(RowNo, PopulationID, PopulationName, PopulationSize, Sort, PopulationProportion, ProportionPercentage)
				SELECT A.RowNo, A.PopulationID, A.PopulationName, ISNULL(B.PopuSize, 0)
				, ISNULL(B.PopuSizeSort, 1)
				, ISNULL(B.Proportion, ''), ISNULL(B.ProportionPercentage, '')
				FROM 
				(
					SELECT pop.PopulationID, pop.PopulationName, ref.RowNo
					FROM genList.uvw_Populations pop
					INNER JOIN 
					(
						SELECT * FROM dbo.utf_SplitStringIncludeRowNo(@MashUpPopuIDs, ',')
					) ref ON ref.col=pop.PopulationID
				) A
				LEFT JOIN (
					SELECT S.RowNo, S.PopuSize
						, PopuSizeSort=ROW_NUMBER() OVER(ORDER BY S.PopuSize DESC)

						, Proportion = CASE WHEN @ListSize > 0 THEN CONVERT(VARCHAR(38), CONVERT(decimal(20, 4), PopuSize*1.0/@ListSize)) ELSE '0.0' END
						, ProportionPercentage = CASE WHEN @ListSize > 0 THEN CONVERT(VARCHAR(38), CONVERT(decimal(20, 4), PopuSize*100.0/@ListSize)) ELSE '0.0' END
						FROM
						(
							SELECT RowNo, PopuSize=CAST(REPLACE(col, ' ', '') AS INT) FROM dbo.utf_SplitStringIncludeRowNo(@MashUpPopuIDsSize, ',')
					) s
				) B ON B.RowNo=A.RowNo
				ORDER BY A.RowNo


				
				IF (@MashUpPopuIDs <> '')
				BEGIN
					SELECT @MashUpPopuNameList = STUFF((
						SELECT '；' + CAST(Sort AS VARCHAR) + '．(' + CAST(PopulationID AS VARCHAR) +  ')' + PopulationName + '：' + Replace(Convert(Varchar(20), CONVERT(money, PopulationSize),1), '.00', '') + ' (' + CAST(ROUND(ProportionPercentage, 2) AS VARCHAR) + '%)'
						FROM #tmpMashUpPopuList
						ORDER BY PopulationSize DESC
						FOR XML PATH('')
					), 1, 1, '')
					, @MashUpPopuIDsProportion = STUFF((
						SELECT ',' + PopulationProportion
						FROM #tmpMashUpPopuList
						ORDER BY RowNo
						FOR XML PATH('')
					), 1, 1, '')
					, @MashUpPopuIDsPercentage = STUFF((
						SELECT ',' + ProportionPercentage
						FROM #tmpMashUpPopuList
						ORDER BY RowNo
						FOR XML PATH('')
					), 1, 1, '')
					, @MashUpPopuIDCount = (SELECT COUNT(*) FROM #tmpMashUpPopuList)

				END  -- IF (@MashUpPopuIDs <> '')
				
				SET @ExecuteDateTo = GETDATE()
				SET @ExecuteSeconds = DATEDIFF(SECOND, @ExecuteDateFrom, @ExecuteDateTo)

				
				PRINT CHAR(9) + 'INSERT INTO logs.MashUp_PopulationUpdateLogs : 記錄更新 Population 名單狀態 --- START --- ' + CONVERT(VARCHAR(20), GETDATE(), 120)
				INSERT INTO logs.MashUp_PopulationUpdateLogs(SourceTable, SourceID, SourceImportDate, MashUp_PopulationSettingID, PopulationID
					, ListSize, MashUpPopuIDs, MashUpPopuIDsSize, MashUpPopuIDsProportion, MashUpPopuIDsPercentage, ListSize_Count, ListSize_Population, ListSize_Rejected, ListSize_Overlap, ListSize_Population_Overlap
					, DestTableName, ExecuteDateFrom, ExecuteDateTo, ExecuteSeconds
					, InsertUser, UpdateUser)
				SELECT @SourceTable, @SourceID, @ImportDate, @MashUp_PopulationSettingID, @PopulationID
					, @ListSize, @MashUpPopuIDs, @MashUpPopuIDsSize, ISNULL(@MashUpPopuIDsProportion, ''), @MashUpPopuIDsPercentage, @ListSize_Count, @ListSize_Population, @ListSize_Rejected, @ListSize_Overlap, @ListSize_Population_Overlap
					, @DestTableName, @ExecuteDateFrom, @ExecuteDateTo, @ExecuteSeconds
					, @SPName, @SPName
				PRINT CHAR(9) + 'INSERT INTO logs.MashUp_PopulationUpdateLogs : 記錄更新 Population 名單狀態 --- START --- ' + CONVERT(VARCHAR(20), GETDATE(), 120) + CHAR(10)


				PRINT CHAR(9) + 'INSERT INTO #tmpPopulationUpdateList --- START --- ' + CONVERT(VARCHAR(20), GETDATE(), 120)
				INSERT INTO #tmpPopulationUpdateList(PopulationID, UpdateDate, ListSizeOri, ListSize_Population, ListSize_Rejected
					, ListSize_Ori_Pre, ListSize_Population_Pre, ListSize_Rejected_Pre, ListSize_Overlap, ListSize_Population_Overlap
					, MashUpPopuIDs, MashUpPopuNameList, MashUpPopuIDsSize, MashUpPopuIDsProportion, MashUpPopuIDsPercentage)
				SELECT @PopulationID, CONVERT(VARCHAR(10), GETDATE(), 111), @ListSize, @ListSize_Population, @ListSize_Rejected
					, @ListSize_Ori_Pre, @ListSize_Population_Pre, @ListSize_Rejected_Pre, @ListSize_Overlap, @ListSize_Population_Overlap
					, @MashUpPopuIDs, @MashUpPopuNameList, @MashUpPopuIDsSize, ISNULL(@MashUpPopuIDsProportion, ''), @MashUpPopuIDsPercentage


				PRINT '※※※ 處理 PopulationID = ' + CAST(@PopulationID AS VARCHAR) + ' --- END --- ' + CONVERT(VARCHAR(20), GETDATE(), 120) + CHAR(10) + CHAR(10)

				DELETE #tmpImport_MashUpPopulation2While WHERE SourceID=@SourceID
				SET @TotalPopulation = (SELECT COUNT(*) FROM #tmpImport_MashUpPopulation2While)

			End	--	While (@TotalPopulation > 0)



			If Exists (SELECT * FROM #tmpPopulationUpdateList)
			Begin
				
				UPDATE ta SET PopulationName=ref.PopulationName
					, OerlapRate_Ori=ListSize_Overlap*100.0/ListSizeOri
					, OerlapRate_Population=ListSize_Population_Overlap*100.0/ListSize_Population
				FROM #tmpPopulationUpdateList ta
				INNER JOIN genList.uvw_Populations ref ON ref.PopulationID=ta.PopulationID

				

				PRINT '發送 Email 通知（１）：過濾 Rejected 後已更新的 Population 資料 --- START --- ' + CONVERT(VARCHAR(20), GETDATE(), 120)

				DECLARE @EmailSubject NVARCHAR(200)
						, @Recipients VARCHAR(500)
						, @Copy_recipients VARCHAR(200)
				DECLARE @bodyHtml varchar(max)

				

				SET @bodyHtml = CAST((
				SELECT td = CAST(PopulationID AS VARCHAR) + '</td><td style="text-align:left;">' + PopulationName 
						  + '</td><td>' + CONVERT(VARCHAR(10), UpdateDate, 111)
						  + '</td><td>' + Replace(Convert(Varchar(20), CONVERT(money, ListSize_Population),1), '.00', '')
						  + '</td><td>' + Replace(Convert(Varchar(20), CONVERT(money, ListSize_Population_Pre),1), '.00', '')
						  + '</td><td>' + Replace(Convert(Varchar(20), CONVERT(money, ListSize_Population_Overlap),1), '.00', '')
						  + '</td><td>' + Replace(Convert(Varchar(20), CONVERT(money, OerlapRate_Population),1), '.00', '') + '%'
						  + '</td><td style="text-align:left; font-size:12px;">' + REPLACE(MashUpPopuNameList, '；', '<br />')
				FROM (
						SELECT PopulationID, PopulationName, UpdateDate, ListSize_Population, ListSize_Population_Pre, ListSize_Population_Overlap, OerlapRate_Population
							, MashUpPopuIDs, MashUpPopuNameList
						FROM #tmpPopulationUpdateList
					  ) AS d
				FOR XML PATH( 'tr' ), TYPE) AS VARCHAR(max))

				SET @bodyHtml = '<table cellpadding="0" cellspacing="0" border="1" style="border:1px #B4C6E7 solid;font-family:Microsoft JhengHei;font-size:14px; text-align:center;">'
										  + '<thead style="text-align:center; background-color:#B4C6E7; color:#000;">'
										  + '<tr><th style="width:150px; text-align:center;">Population ID</th>'
										  + '<th style="width:250px;">Population Name</th><th style="width:100px;">更新日期</th>'
										  + '<th style="width:150px;">更新後的數量</th>'
										  + '<th style="width:150px;">上一次數量</th>'
										  + '<th style="width:150px;">與上一次更新<br />重疊的數量</th>'
										  + '<th style="width:150px; white-space:nowrap;">重疊率<br />（重疊數量/更新數量）</th>'
										  + '<th style="width:250px;">混搭的 Poopulation 清單<br />（項次 - (Popu ID) Popu Name）</th>'
										  + '</tr></thead>'
										  + REPLACE(REPLACE(@bodyHtml, '&lt;', '<' ), '&gt;', '>')
										  + '</table>'
				
				SET @bodyHtml = '<div style="font-family:Microsoft JhengHei;font-size:14px;">'
							  + '<div>日期：' + CONVERT(VARCHAR(10), GETDATE(), 111) + '</div><br />'
							  + '（此為過濾掉 Rejected 名單後，更新至 Population 的數據）<br />'
							  + @bodyHtml + '<br /><br /><div style="font-family:Microsoft JhengHei;font-size:14px; color:gray;"><br /><br />執行程式 : 【210】' + @SPName + '</div>'
							  + '</div>'

				PRINT '@bodyHtml (已濾 Rejected) = ' + CHAR(10) + @bodyHtml
				



				SET @EmailSubject = '混搭型 Population 更新通知：' + CONVERT(VARCHAR(10), GETDATE(), 111)
				SET @Recipients = (SELECT SysText FROM genList.uvw_SysCode WHERE [SysName]='Email.Recipients.MashUpPopulation' AND SysValue='Account')
				SET @Copy_recipients = (SELECT SysText FROM genList.uvw_SysCode WHERE [SysName]='Email.Recipients.MashUpPopulation' AND SysValue='Management') + ';' + (SELECT SysText FROM genList.uvw_SysCode WHERE [SysName]='Email.Recipients.MashUpPopulation' AND SysValue='IT')
				SET @Copy_recipients = REPLACE(@Copy_recipients, ';;', ';')


				exec msdb.dbo.sp_send_dbmail
						@profile_name = '我的設定檔',   --設定檔
						@recipients = @Recipients,
						@Copy_recipients = @Copy_recipients,
						@Blind_copy_recipients = '',
						@subject = @EmailSubject,  --主旨
						@body = @bodyHtml, --內文
						@body_format = 'HTML',  -- TEXT / HTML  格式
						@file_attachments = ''


				PRINT '發送 Email 通知（１）：過濾 Rejected 後已更新的 Population 資料 --- END --- '+ CONVERT(VARCHAR(20), GETDATE(), 120) + CHAR(10) + CHAR(10)

			End	--	If Exists (SELECT * FROM #tmpPopulationUpdateList)
			
		END	--	IF EXISTS (SELECT * FROM #tmpImport_MashUpPopulation)

		
		
		

		
	END TRY
	BEGIN CATCH
		
		print '寫入錯誤訊息 = ' + ERROR_MESSAGE()
		Exec dbo.usp_InsertErrorLogProcedure @User=@SPName, @Note=NULL

	END CATCH
	

	/* 處理使用外部組件【MobiStringSplit】的函數【dbo.utf_clr_SplitStringIncludeRowNo】安全性問題（關閉） */
	ALTER DATABASE [我的資料庫] SET TRUSTWORTHY OFF




	IF OBJECT_ID('tempdb..#tmpImport_MashUpPopulation') IS NOT NULL DROP TABLE #tmpImport_MashUpPopulation
	IF OBJECT_ID('tempdb..#tmpImport_MashUpPopulation2While') IS NOT NULL DROP TABLE #tmpImport_MashUpPopulation2While
	IF OBJECT_ID('tempdb..#tmpUIDList') IS NOT NULL DROP TABLE #tmpUIDList
	IF OBJECT_ID('tempdb..#tmpUIDList_Pre') IS NOT NULL DROP TABLE #tmpUIDList_Pre
	IF OBJECT_ID('tempdb..#tmpMashUpPopuList') IS NOT NULL DROP TABLE #tmpMashUpPopuList
	IF OBJECT_ID('tempdb..#tmpPopulationUpdateList') IS NOT NULL DROP TABLE #tmpPopulationUpdateList
	IF OBJECT_ID('tempdb..#tmpToSendAlertEmail') IS NOT NULL DROP TABLE #tmpToSendAlertEmail


END


GO

