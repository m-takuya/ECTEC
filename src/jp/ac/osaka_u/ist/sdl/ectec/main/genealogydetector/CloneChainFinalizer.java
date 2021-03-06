package jp.ac.osaka_u.ist.sdl.ectec.main.genealogydetector;

import java.util.ArrayList;
import java.util.List;

import jp.ac.osaka_u.ist.sdl.ectec.db.data.DBCloneGenealogyInfo;
import jp.ac.osaka_u.ist.sdl.ectec.db.data.DBCloneSetInfo;
import jp.ac.osaka_u.ist.sdl.ectec.db.data.DBCloneSetLinkInfo;
import jp.ac.osaka_u.ist.sdl.ectec.db.data.retriever.AbstractElementRetriever;
import jp.ac.osaka_u.ist.sdl.ectec.db.data.retriever.ILinkElementRetriever;

public class CloneChainFinalizer
		extends
		ElementChainFinalizer<DBCloneSetInfo, DBCloneSetLinkInfo, DBCloneGenealogyInfo> {

	// private final long lastRevisionId;

	public CloneChainFinalizer(
			AbstractElementRetriever<DBCloneSetInfo> elementRetriever,
			ILinkElementRetriever<DBCloneSetLinkInfo> linkRetriever) {
		super(elementRetriever, linkRetriever);
		// this.lastRevisionId = lastRevisionId;
	}

	@Override
	protected DBCloneGenealogyInfo createInstanceFromChain(
			ElementChain<DBCloneSetLinkInfo> chain) throws Exception {
		final long startRevisionId = chain.getFirstRevision();
		final long endRevisionId = chain.getLastRevision();
		final List<Long> elements = new ArrayList<Long>();
		elements.addAll(chain.getElements());
		final List<Long> links = new ArrayList<Long>();
		links.addAll(chain.getLinks());

		// final Map<Long, DBCloneSetLinkInfo> linksMap = linkRetriever
		// .retrieveWithIds(links);
		// int numberOfChanges = 0;
		// int numberOfAdditions = 0;
		// int numberOfDeletions = 0;
		//
		// for (final DBCloneSetLinkInfo link : linksMap.values()) {
		// if (link.getNumberOfChangedElements() > 0) {
		// numberOfChanges++;
		// }
		// if (link.getNumberOfAddedElements() > 0) {
		// numberOfAdditions++;
		// }
		// if (link.getNumberOfDeletedElements() > 0) {
		// numberOfDeletions++;
		// }
		// }
		//
		// final boolean dead = (endRevisionId == lastRevisionId);

		// return new DBCloneGenealogyInfo(startRevisionId, endRevisionId,
		// elements, links);
		return new DBCloneGenealogyInfo(startRevisionId, endRevisionId,
				elements, links);
	}

	@Override
	protected DBCloneGenealogyInfo createInstanceFromElement(
			DBCloneSetInfo element) {
		final long startRevisionId = element.getCombinedRevisionId();
		final long endRevisionId = element.getCombinedRevisionId();
		final List<Long> elements = new ArrayList<Long>();
		final List<Long> links = new ArrayList<Long>();
		elements.add(element.getId());

		// final int numberOfChanges = 0;
		// final int numberOfAdditions = 0;
		// final int numberOfDeletions = 0;
		//
		// final boolean dead = (endRevisionId == lastRevisionId);

		return new DBCloneGenealogyInfo(startRevisionId, endRevisionId,
				elements, links);
	}

}
