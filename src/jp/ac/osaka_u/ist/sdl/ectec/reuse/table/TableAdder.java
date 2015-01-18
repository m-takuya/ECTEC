package jp.ac.osaka_u.ist.sdl.ectec.reuse.table;

import jp.ac.osaka_u.ist.sdl.ectec.LoggingManager;
import jp.ac.osaka_u.ist.sdl.ectec.db.DBConnectionManager;

import org.apache.log4j.Logger;

/**
 * A class that performs the main process of create additional tables
 *
 * @author m-takuya
 *
 */
public class TableAdder {

	/**
	 * the logger
	 */
	private static final Logger logger = LoggingManager
			.getLogger(TableAdder.class.getName());

	/**
	 * the db manager
	 */
	private final DBConnectionManager dbManager;

	public TableAdder(final DBConnectionManager dbManager) {
		this.dbManager = dbManager;
	}

	public void perform() throws Exception {
		dbManager.setAutoCommit(true);
		logger.info("adding new tables");

		addTables();

		// TODO createIndex(余裕があれば)

		logger.info("complete");
		dbManager.setAutoCommit(false);
	}

	private void addTables() throws Exception{
		dbManager.executeUpdate(getDeveloperTableQuery());
		dbManager.executeUpdate(getAuthorTableQuery());
		dbManager.executeUpdate(getReuseTableQuery());
	}
	/**
	 * get the query to create the reuse table
	 *
	 * @return
	 */
	private String getDeveloperTableQuery() {
		final StringBuilder builder = new StringBuilder();

		builder.append("create table DEVELOPER(");
		builder.append("DEVELOPER_ID BIGINT PRIMARY KEY,");
		builder.append("NAME TEXT,");
		builder.append("REPOSITORY_ID BIGINT,");
		builder.append("COMMIT BIGINT,");
		builder.append("FOREIGN KEY(REPOSITORY_ID) REFERENCES REPOSITORY(REPOSITORY_ID)");
		builder.append(")");

		return builder.toString();
	}

	/**
	 * get the query to create the reuse table
	 *
	 * @return
	 */
	private String getAuthorTableQuery() {
		final StringBuilder builder = new StringBuilder();

		builder.append("create table AUTHOR(");
		builder.append("CLONE_SET_ID BIGINT,");
		builder.append("CODE_FRAGMENT_ID BIGINT,");
		builder.append("DEVELOPER_ID BIGINT,");
		builder.append("PRIMARY KEY(CLONE_SET_ID, CODE_FRAGMENT_ID),");
		builder.append("FOREIGN KEY(DEVELOPER_ID) REFERENCES DEVELOPER(DEVELOPER_ID)");
		builder.append(")");

		return builder.toString();
	}

	/**
	 * get the query to create the reuse table
	 *
	 * @return
	 */
	private String getReuseTableQuery() {
		final StringBuilder builder = new StringBuilder();

		builder.append("create table REUSE(");
		builder.append("REUSE_ID BIGINT PRIMARY KEY,");
		builder.append("CLONE_SET_ID BIGINT,");
		builder.append("REUSED_CODE_FRAGMENT_ID BIGINT,"); //再利用されたコード片
		builder.append("REUSE_REVISION_ID BIGINT,"); //いつ再利用されたか
		builder.append("REUSE_DEVELOPER_ID BIGINT,"); //誰が再利用したか
		builder.append("FOREIGN KEY(CLONE_SET_ID, REUSED_CODE_FRAGMENT_ID) REFERENCES CLONE_SET(CLONE_SET_ID, ELEMENT),");
		builder.append("FOREIGN KEY(REUSED_CODE_FRAGMENT_ID) REFERENCES CODE_FRAGMENT(CODE_FRAGMENT_ID),");
		builder.append("FOREIGN KEY(REUSE_REVISION_ID) REFERENCES REVISION(REVISION_ID),");
		builder.append("FOREIGN KEY(REUSE_DEVELOPER_ID) REFERENCES DEVELOPER(DEVELOPER_ID)");
		builder.append(")");

		return builder.toString();
	}



}
