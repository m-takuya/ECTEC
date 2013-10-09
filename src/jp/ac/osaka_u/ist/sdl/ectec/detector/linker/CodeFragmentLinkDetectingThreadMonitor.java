package jp.ac.osaka_u.ist.sdl.ectec.detector.linker;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;

import jp.ac.osaka_u.ist.sdl.ectec.db.data.DBCodeFragmentInfo;
import jp.ac.osaka_u.ist.sdl.ectec.db.data.DBCodeFragmentLinkInfo;
import jp.ac.osaka_u.ist.sdl.ectec.db.data.DBCommitInfo;
import jp.ac.osaka_u.ist.sdl.ectec.db.data.DBCrdInfo;
import jp.ac.osaka_u.ist.sdl.ectec.db.data.registerer.CodeFragmentLinkRegisterer;
import jp.ac.osaka_u.ist.sdl.ectec.settings.Constants;
import jp.ac.osaka_u.ist.sdl.ectec.settings.MessagePrinter;

/**
 * A monitor class for code clone link threads
 * 
 * @author k-hotta
 * 
 */
public class CodeFragmentLinkDetectingThreadMonitor {

	/**
	 * a map having detected links
	 */
	private final ConcurrentMap<Long, DBCodeFragmentLinkInfo> detectedLinks;

	/**
	 * the registerer for links of code fragments
	 */
	private final CodeFragmentLinkRegisterer fragmentLinkRegisterer;

	/**
	 * the map between revision id and code fragments included in the revision
	 */
	private final ConcurrentMap<Long, Map<Long, DBCodeFragmentInfo>> codeFragments;

	/**
	 * the map between revision id and crds included in the revision
	 */
	private final ConcurrentMap<Long, Map<Long, DBCrdInfo>> crds;

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

	public CodeFragmentLinkDetectingThreadMonitor(
			final ConcurrentMap<Long, DBCodeFragmentLinkInfo> detectedLinks,
			final CodeFragmentLinkRegisterer fragmentLinkRegisterer,
			final ConcurrentMap<Long, Map<Long, DBCodeFragmentInfo>> codeFragments,
			final ConcurrentMap<Long, Map<Long, DBCrdInfo>> crds,
			final ConcurrentMap<Long, DBCommitInfo> processedCommits,
			final Map<Long, Collection<Long>> revisionAndRelatedCommits,
			final int maxElementsCount) {
		this.detectedLinks = detectedLinks;
		this.fragmentLinkRegisterer = fragmentLinkRegisterer;
		this.codeFragments = codeFragments;
		this.crds = crds;
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
				synchronized (detectedLinks) {
					if (detectedLinks.size() >= maxElementsCount) {
						final Set<DBCodeFragmentLinkInfo> currentElements = new HashSet<DBCodeFragmentLinkInfo>();
						currentElements.addAll(detectedLinks.values());
						fragmentLinkRegisterer.register(currentElements);
						MessagePrinter
								.println("\t"
										+ currentElements.size()
										+ " links of fragments have been registered into db");
						numberOfLinks += currentElements.size();

						for (final DBCodeFragmentLinkInfo link : currentElements) {
							detectedLinks.remove(link.getId());
						}
					}
				}

				// remove fragments if they are no longer needed
				synchronized (codeFragments) {
					final Collection<Long> fragmentRevisionIds = new TreeSet<Long>();
					fragmentRevisionIds.addAll(codeFragments.keySet());
					for (final long revisionId : fragmentRevisionIds) {
						final Collection<Long> relatedCommits = revisionAndRelatedCommits
								.get(revisionId);
						if (processedCommits.keySet().containsAll(
								relatedCommits)) {
							codeFragments.remove(revisionId);
						}
					}
				}

				// remove crds if they are no longer needed
				synchronized (crds) {
					final Collection<Long> crdRevisionIds = new TreeSet<Long>();
					crdRevisionIds.addAll(crds.keySet());
					for (final long revisionId : crdRevisionIds) {
						final Collection<Long> relatedCommits = revisionAndRelatedCommits
								.get(revisionId);
						if (processedCommits.keySet().containsAll(
								relatedCommits)) {
							crds.remove(revisionId);
						}
					}
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

			// break this loop if all the other threads have died
			if (Thread.activeCount() == 2) {
				break;
			}

		}

		MessagePrinter.println();

		MessagePrinter.println("\tall threads have finished their work");
		MessagePrinter
				.println("\tregistering all the remaining elements into db ");
		fragmentLinkRegisterer.register(detectedLinks.values());

		numberOfLinks += detectedLinks.size();

		MessagePrinter.println("\t\tOK");

		MessagePrinter.println();

		MessagePrinter.println("the numbers of detected elements are ... ");
		MessagePrinter.println("\tLinks: " + numberOfLinks);

	}
}