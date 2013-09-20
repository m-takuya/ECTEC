package jp.ac.osaka_u.ist.sdl.ectec.data.registerer;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import jp.ac.osaka_u.ist.sdl.ectec.data.CodeFragmentLinkInfo;
import jp.ac.osaka_u.ist.sdl.ectec.db.DBConnectionManager;

/**
 * A class that represents a registerer for links of code fragments
 * 
 * @author k-hotta
 * 
 */
public class CodeFragmentLinkRegisterer extends
		AbstractElementRegisterer<CodeFragmentLinkInfo> {

	/**
	 * the constructor
	 * 
	 * @param dbManager
	 * @param maxBatchCount
	 */
	public CodeFragmentLinkRegisterer(DBConnectionManager dbManager,
			int maxBatchCount) {
		super(dbManager, maxBatchCount);
	}

	@Override
	protected String createPreparedStatementQueue() {
		return "insert into CODE_FRAGMENT_LINK values(?,?,?,?,?,?)";
	}

	@Override
	protected void setAttributes(PreparedStatement pstmt,
			CodeFragmentLinkInfo element) throws SQLException {
		int column = 0;
		pstmt.setLong(++column, element.getId());
		pstmt.setLong(++column, element.getBeforeElementId());
		pstmt.setLong(++column, element.getAfterElementId());
		pstmt.setLong(++column, element.getBeforeRevisionId());
		pstmt.setLong(++column, element.getAfterRevisionId());
		pstmt.setInt(++column, (element.isChanged()) ? 1 : 0);
	}

}