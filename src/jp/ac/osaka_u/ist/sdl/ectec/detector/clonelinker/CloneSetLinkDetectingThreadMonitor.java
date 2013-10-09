package jp.ac.osaka_u.ist.sdl.ectec.detector.clonelinker;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;

import jp.ac.osaka_u.ist.sdl.ectec.db.data.DBCloneSetInfo;
import jp.ac.osaka_u.ist.sdl.ectec.db.data.DBCloneSetLinkInfo;
import jp.ac.osaka_u.ist.sdl.ectec.db.data.DBCommitInfo;
import jp.ac.osaka_u.ist.sdl.ectec.db.data.registerer.CloneSetLinkRegisterer;
import jp.ac.osaka_u.ist.sdl.ectec.settings.Constants;
import jp.ac.osaka_u.ist.sdl.ectec.settings.MessagePrinter;

/**
 * A monitor class for clone set linking threads
 * 
 * @author k-hotta
 * 
 */
public class CloneSetLinkDetectingThreadMonitor {

	/**
	 * a map having detected links of clones
	 */
	private final ConcurrentMap<Long, DBCloneSetLinkInfo> detectedCloneLinks;

	/**
	 * the registerer for clone sets
	 */
	private final CloneSetLinkRegisterer cloneLinkRegisterer;

	/**
	 * the map between revision id and clone sets including in the revision
	 */
	private final ConcurrentMap<Long, Map<Long, DBCloneSetInfo>> cloneSets;

	/**
	 * already processed commits
	 */
	private final ConcurrentMap<Long, DBCommitInfo> processedCommits;

	/**
	 * id of a revision and a collection of ids of commits that relates to the
	 * revision
	 */
	private final Map<Long, Collection<Long>> revisionAndRelatedCommits;

	/**
	 * the threshold for elements <br>
	 * if the number of stored elements exceeds this threshold, then this
	 * monitor interrupts the other threads and register elements into db with
	 * the registered elements removed from the map
	 */
	private final int maxElementsCount;

	public CloneSetLinkDetectingThreadMonitor(
			final ConcurrentMap<Long, DBCloneSetLinkInfo> detectedCloneLinks,
			final CloneSetLinkRegisterer cloneLinkRegisterer,
			final ConcurrentMap<Long, Map<Long, DBCloneSetInfo>> cloneSets,
			final ConcurrentMap<Long, DBCommitInfo> processedCommits,
			final Map<Long, Collection<Long>> revisionAndRelatedCommits,
			final int maxElementsCount) {
		this.detectedCloneLinks = detectedCloneLinks;
		this.cloneLinkRegisterer = cloneLinkRegisterer;
		this.cloneSets = cloneSets;
		this.processedCommits = processedCommits;
		this.revisionAndRelatedCommits = revisionAndRelatedCommits;
		this.maxElementsCount = maxElementsCount;
	}

	public void monitor() throws Exception {
		int numberOfLinks = 0;

		while (true) {
			try {
				Thread.sleep(Constants.MONITORING_INTERVAL);

				// checking the number of detected links
				synchronized (detectedCloneLinks) {
					if (detectedCloneLinks.size() >= maxElementsCount) {
						final Set<DBCloneSetLinkInfo> currentElements = new HashSet<DBCloneSetLinkInfo>();
						currentElements.addAll(detectedCloneLinks.values());
						cloneLinkRegisterer.register(currentElements);
						MessagePrinter
								.println("\t"
										+ currentElements.size()
										+ " links of fragments have been registered into db");
						numberOfLinks += currentElements.size();

						for (final DBCloneSetLinkInfo link : currentElements) {
							detectedCloneLinks.remove(link.getId());
						}
					}
				}

				// remove clones if they are no longer needed
				synchronized (cloneSets) {
					final Collection<Long> cloneRevisionIds = new TreeSet<Long>();
					cloneRevisionIds.addAll(cloneSets.keySet());
					for (final long revisionId : cloneRevisionIds) {
						final Collection<Long> relatedCommits = revisionAndRelatedCommits
								.get(revisionId);
						if (processedCommits.keySet().containsAll(
								relatedCommits)) {
							cloneSets.remove(revisionId);
						}
					}
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

			if (Thread.activeCount() == 2) {
				break;
			}
		}

		MessagePrinter.println();

		MessagePrinter.println("\tall threads have finished their work");
		MessagePrinter
				.println("\tregistering all the remaining elements into db ");
		cloneLinkRegisterer.register(detectedCloneLinks.values());

		numberOfLinks += detectedCloneLinks.size();

		MessagePrinter.println("\t\tOK");

		MessagePrinter.println();

		MessagePrinter.println("the numbers of detected elements are ... ");
		MessagePrinter.println("\tLinks: " + numberOfLinks);
	}

}