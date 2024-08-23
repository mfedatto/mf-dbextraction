$serverInstance = "SVRDEV001"
$databaseName = "PAS_INTEGRATION"
$username = "LSUserDev001"
$password = "E@3do9S5dadj"
$securePassword = ConvertTo-SecureString $password -AsPlainText -Force
$credential = New-Object System.Management.Automation.PSCredential ($username, $securePassword)
$outputFilePath = "C:\Users\mauricio.fedatto\repos\__dropfolder\migation\PAS_INTEGRATION\db\migrations\versioned\V20240814.0.1__Create_tables_and_constraints.sql"
$connectionString = "Server=$serverInstance;Database=$databaseName;User Id=$username;Password=$password;TrustServerCertificate=True;"

$ignorePrimaryKeyConstraints = $true
$ignoreUniqueConstraints = $true

Add-Type -AssemblyName "Microsoft.SqlServer.SMO"

Write-Host "Obtendo indices..."
$indexesQuery = @"
SELECT
    t.name AS TableName,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    c.name AS ColumnName
FROM
    sys.indexes AS i
    INNER JOIN sys.index_columns AS ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    INNER JOIN sys.columns AS c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    INNER JOIN sys.tables AS t ON i.object_id = t.object_id
WHERE t.is_ms_shipped = 0
ORDER BY t.name, i.name, ic.key_ordinal
"@
$indexes = Invoke-Sqlcmd -ConnectionString $connectionString -Query $indexesQuery

Write-Host "Conectando ao servidor e banco de dados..."
$server = New-Object Microsoft.SqlServer.Management.Smo.Server($serverInstance)
$server.ConnectionContext.LoginSecure = $false
$server.ConnectionContext.set_Login($username)
$server.ConnectionContext.set_SecurePassword($securePassword)
$database = $server.Databases[$databaseName]

$script = ""

Write-Host "Gerando script para tabelas..."
foreach ($table in $database.Tables) {
    $tableVar = "
DECLARE @Table_$($table.Name)_created BIT = 0;"
    $script += $tableVar
    $tableScript = $table.Script() -join [Environment]::NewLine

    $script += "

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '$($table.Name)') 
BEGIN

$tableScript
SET @Table_$($table.Name)_created = 1;

PRINT 'Tabela $($table.Name) criada'
"

    $tableIndexes = $indexes | Where-Object { $_.TableName -eq $table.Name }

    $processedIndexes = @{}
    foreach ($index in $tableIndexes) {
        if (-not $processedIndexes.ContainsKey($index.IndexName)) {
            $script += "
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '$($index.IndexName)' AND object_id = OBJECT_ID('$($table.Schema).$($table.Name)'))
BEGIN
    CREATE INDEX [$($index.IndexName)] ON [$($table.Schema)].[$($table.Name)] ($($index.ColumnName));
    PRINT 'Indice [$($index.IndexName)] criado para tabela $($table.Name)'
END
"
            $processedIndexes[$index.IndexName] = $true
        }
    }

    $script += "
END
"
}

Write-Host "Obtendo constraints..."
$constraintsQuery = @"
SELECT
    tc.TABLE_NAME AS TableName,
    tc.CONSTRAINT_NAME AS ConstraintName,
    tc.CONSTRAINT_TYPE AS ConstraintType,
    kcu.COLUMN_NAME AS ColumnName,
    rc.REFERENCED_TABLE_NAME AS ReferencedTableName,
    rc.REFERENCED_COLUMN_NAME AS ReferencedColumnName
FROM
    INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
    LEFT JOIN (
        SELECT
            rc.CONSTRAINT_NAME,
            kcu.COLUMN_NAME AS REFERENCED_COLUMN_NAME,
            c.TABLE_NAME AS REFERENCED_TABLE_NAME
        FROM
            INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS rc
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
            ON rc.UNIQUE_CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            INNER JOIN INFORMATION_SCHEMA.TABLES AS c
            ON kcu.TABLE_NAME = c.TABLE_NAME
        WHERE
            rc.CONSTRAINT_NAME IS NOT NULL
    ) AS rc
    ON tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
WHERE
    tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'CHECK')
ORDER BY tc.TABLE_NAME, tc.CONSTRAINT_NAME
"@
$constraints = Invoke-Sqlcmd -ConnectionString $connectionString -Query $constraintsQuery

$processedConstraints = @{}
foreach ($constraint in $constraints) {
    if (-not $processedConstraints.ContainsKey($constraint.ConstraintName)) {
        $constraintScript = ""

        if ($constraint.ConstraintType -eq "PRIMARY KEY" -and $ignorePrimaryKeyConstraints) {
            continue
        }

        if ($constraint.ConstraintType -eq "UNIQUE" -and $ignoreUniqueConstraints) {
            continue
        }

        switch ($constraint.ConstraintType) {
            "PRIMARY KEY" {
                $constraintScript = "
IF @Table_$($constraint.TableName)_created = 1 AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = '$($constraint.ConstraintName)')
BEGIN

ALTER TABLE [$($constraint.TableName)]
ADD CONSTRAINT [$($constraint.ConstraintName)] PRIMARY KEY ($($constraint.ColumnName));
PRINT 'Chave primaria [$($constraint.ConstraintName)] criada para tabela $($constraint.TableName)'

END
"
            }
            "FOREIGN KEY" {
                $constraintScript = "
IF @Table_$($constraint.TableName)_created = 1 AND @Table_$($constraint.ReferencedTableName)_created = 1 AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = '$($constraint.ConstraintName)')
BEGIN

ALTER TABLE [$($constraint.TableName)]
ADD CONSTRAINT [$($constraint.ConstraintName)] FOREIGN KEY ($($constraint.ColumnName))
REFERENCES [$($constraint.ReferencedTableName)] ($($constraint.ReferencedColumnName));
PRINT 'Chave estrangeira [$($constraint.ConstraintName)] criada para tabela $($constraint.TableName)'

END
"
            }
            "UNIQUE" {
                $constraintScript = "
IF @Table_$($constraint.TableName)_created = 1 AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = '$($constraint.ConstraintName)')
BEGIN

ALTER TABLE [$($constraint.TableName)]
ADD CONSTRAINT [$($constraint.ConstraintName)] UNIQUE ($($constraint.ColumnName));
PRINT 'Constraint unica [$($constraint.ConstraintName)] criada para tabela $($constraint.TableName)'

END
"
            }
            "CHECK" {
                $constraintScript = "
IF @Table_$($constraint.TableName)_created = 1 AND NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = '$($constraint.ConstraintName)')
BEGIN

ALTER TABLE [$($constraint.TableName)]
ADD CONSTRAINT [$($constraint.ConstraintName)] CHECK ($($constraint.ColumnName));
PRINT 'Constraint de verificação [$($constraint.ConstraintName)] criada para tabela $($constraint.TableName)'

END
"
            }
        }

        $script += $constraintScript
        $processedConstraints[$constraint.ConstraintName] = $true
    }
}

function Remove-SetLines {
    param (
        [string[]]$scriptLines
    )

    $foundSetAnsiNulls = $false
    $foundSetQuotedIdentifier = $false

    $filteredLines = @()
    
    foreach ($line in $scriptLines) {
        if ($line -match "SET ANSI_NULLS ON") {
            if (-not $foundSetAnsiNulls) {
                $foundSetAnsiNulls = $true
                continue
            }
        }

        if ($line -match "SET QUOTED_IDENTIFIER ON") {
            if (-not $foundSetQuotedIdentifier) {
                $foundSetQuotedIdentifier = $true
                continue
            }
        }

        $filteredLines += $line
    }

    return $filteredLines
}

function Remove-InitialSetCommands {
    param (
        [string]$script
    )
    
    $lines = $script -split "`r`n"
    $output = @()
    $foundFirstNonEmptyLine = $false
    $removeNextLine = $false
    
    foreach ($line in $lines) {
        $trimmedLine = $line.Trim()
        
        if (-not $foundFirstNonEmptyLine -and [string]::IsNullOrWhiteSpace($trimmedLine)) {
            $output += $line
            continue
        }

        if (-not $foundFirstNonEmptyLine) {
            $foundFirstNonEmptyLine = $true
        }

        if (-not $removeNextLine -and $trimmedLine -eq "SET ANSI_NULLS ON") {
            $removeNextLine = $true
            continue
        }
        
        if ($removeNextLine -and $trimmedLine -eq "SET QUOTED_IDENTIFIER ON") {
            $removeNextLine = $false
            continue
        }
        
        $output += $line
    }
    
    return $output -join "`r`n"
}

Write-Host "Gerando script para views, stored procedures e funcoes..."
$views = $database.Views | Where-Object { $_.IsSystemObject -eq $false }
$storedProcedures = $database.StoredProcedures | Where-Object { $_.IsSystemObject -eq $false }
$functions = $database.UserDefinedFunctions | Where-Object { $_.IsSystemObject -eq $false }

foreach ($view in $views) {
    $script += "
IF NOT EXISTS (SELECT 1 FROM sys.views WHERE name = '$($view.Name)')
BEGIN

$($view.Script() -join [Environment]::NewLine)

PRINT 'View $($view.Name) criada'

END
"
}

foreach ($storedProcedure in $storedProcedures) {
    $script += "
IF NOT EXISTS (SELECT 1 FROM sys.procedures WHERE name = '$($storedProcedure.Name)')
BEGIN

$($storedProcedure.Script() -join [Environment]::NewLine)

PRINT 'Stored procedure $($storedProcedure.Name) criada'

END
"
}

foreach ($function in $functions) {
    $script += "
IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE type = 'FN' AND name = '$($function.Name)')
BEGIN

$($function.Script() -join [Environment]::NewLine)

PRINT 'Function $($function.Name) criada'

END
"
}

Write-Host "Escrevendo script no arquivo $outputFilePath..."
$script | Out-File -FilePath $outputFilePath -Encoding UTF8

Write-Host "Script gerado com sucesso."
