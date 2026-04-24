class TablespaceManager {
  buildSnapshot(rows = [], source) {
    const grouped = new Map();

    rows.forEach((row) => {
      const tablespaceName = row.TABLESPACE_NAME;
      if (!grouped.has(tablespaceName)) {
        grouped.set(tablespaceName, {
          tablespaceName,
          status: row.STATUS || 'UNKNOWN',
          contents: row.CONTENTS || 'PERMANENT',
          bigfile: row.BIGFILE || 'NO',
          extentManagement: row.EXTENT_MANAGEMENT || null,
          segmentSpaceManagement: row.SEGMENT_SPACE_MANAGEMENT || null,
          datafiles: []
        });
      }

      grouped.get(tablespaceName).datafiles.push({
        fileId: Number(row.FILE_ID || 0),
        fileName: row.FILE_NAME,
        sizeMb: Number(row.SIZE_MB || 0),
        maxSizeMb: Number(row.MAX_SIZE_MB || 0),
        autoextensible: String(row.AUTOEXTENSIBLE || 'NO') === 'YES',
        incrementByBlocks: Number(row.INCREMENT_BY_BLOCKS || 0)
      });
    });

    return {
      source,
      tablespaces: Array.from(grouped.values())
        .sort((a, b) => a.tablespaceName.localeCompare(b.tablespaceName))
        .map((tablespace) => ({
          ...tablespace,
          datafiles: tablespace.datafiles.sort((a, b) => a.fileId - b.fileId)
        }))
    };
  }

  async getTablespacesAndDatafiles(oracleDb) {
    try {
      const result = await oracleDb.execute(`
        SELECT
          df.tablespace_name,
          ts.status,
          ts.contents,
          ts.bigfile,
          ts.extent_management,
          ts.segment_space_management,
          df.file_id,
          df.file_name,
          ROUND(df.bytes / 1024 / 1024, 2) AS size_mb,
          ROUND(CASE WHEN df.maxbytes > 0 THEN df.maxbytes ELSE df.bytes END / 1024 / 1024, 2) AS max_size_mb,
          df.autoextensible,
          df.increment_by AS increment_by_blocks
        FROM dba_data_files df
        JOIN dba_tablespaces ts
          ON ts.tablespace_name = df.tablespace_name
        ORDER BY df.tablespace_name, df.file_id
      `);

      return this.buildSnapshot(result.rows || [], 'dba_data_files');
    } catch (primaryError) {
      try {
        const fallback = await oracleDb.execute(`
          SELECT
            ts.name AS tablespace_name,
            'ONLINE' AS status,
            NULL AS contents,
            NULL AS bigfile,
            NULL AS extent_management,
            NULL AS segment_space_management,
            df.file# AS file_id,
            df.name AS file_name,
            ROUND(df.bytes / 1024 / 1024, 2) AS size_mb,
            ROUND(df.bytes / 1024 / 1024, 2) AS max_size_mb,
            'UNKNOWN' AS autoextensible,
            0 AS increment_by_blocks
          FROM v$datafile df
          JOIN v$tablespace ts
            ON ts.ts# = df.ts#
          ORDER BY ts.name, df.file#
        `);

        return this.buildSnapshot(fallback.rows || [], 'v$datafile');
      } catch (fallbackError) {
        throw new Error(`Unable to read tablespaces/datafiles. ${fallbackError.message}`);
      }
    }
  }

  async getDatafileByName(oracleDb, fileName) {
    const snapshot = await this.getTablespacesAndDatafiles(oracleDb);

    for (const tablespace of snapshot.tablespaces) {
      const datafile = tablespace.datafiles.find((item) => item.fileName === fileName);
      if (datafile) {
        return {
          ...datafile,
          tablespaceName: tablespace.tablespaceName
        };
      }
    }

    throw new Error(`Datafile not found: ${fileName}`);
  }
}

module.exports = new TablespaceManager();
