package jp.ac.osaka_u.ist.sdl.ectec.main.fragmentdetector;

import java.sql.SQLException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;

import jp.ac.osaka_u.ist.sdl.ectec.LoggingManager;
import jp.ac.osaka_u.ist.sdl.ectec.db.data.DBCodeFragmentInfo;
import jp.ac.osaka_u.ist.sdl.ectec.db.data.DBCrdInfo;
import jp.ac.osaka_u.ist.sdl.ectec.db.data.registerer.CRDRegisterer;
import jp.ac.osaka_u.ist.sdl.ectec.db.data.registerer.CodeFragmentRegisterer;
import jp.ac.osaka_u.ist.sdl.ectec.settings.Constants;

import org.apache.log4j.Logger;

/**
 * A monitor class for code fragment detecting threads
 * 
 * @author k-hotta
 * 
 */
public class CodeFragmentDetectingThreadMonitor {

	/**
	 * the logger
	 */
	private static final Logger logger = LoggingManager
			.getLogger(CodeFragmentDetectingThreadMonitor.class.getName());

	/**
	 * the logger for errors
	 */
	private final static Logger eLogger = LoggingManager.getLogger("error");

	/**
	 * a map having detected crds
	 */
	private final ConcurrentMap<Long, DBCrdInfo> detectedCrds;

	/**
	 * a map having detected fragments
	 */
	private final ConcurrentMap<Long, DBCodeFragmentInfo> detectedFragments;

	/**
	 * the threshold for elements <br>
	 * if the number of stored elements exceeds this threshold, then this
	 * monitor interrupts the other threads and register elements into db with
	 * the registered elements removed from the map
	 */
	private final int maxElementsCount;

	/**
	 * the registerer for crds
	 */
	private final CRDRegisterer crdRegisterer;

	/**
	 * the registerer for code fragments
	 */
	private final CodeFragmentRegisterer fragmentRegisterer;

	/**
	 * the array of the threads to be monitored
	 */
	private final Thread[] threads;

	public CodeFragmentDetectingThreadMonitor(
			final ConcurrentMap<Long, DBCrdInfo> detectedCrds,
			final ConcurrentMap<Long, DBCodeFragmentInfo> detectedFragments,
			final int maximumElementsCount, final CRDRegisterer crdRegisterer,
			final CodeFragmentRegisterer fragmentRegisterer,
			final Thread[] threads) {
		this.detectedCrds = detectedCrds;
		this.detectedFragments = detectedFragments;
		this.maxElementsCount = maximumElementsCount;
		this.crdRegisterer = crdRegisterer;
		this.fragmentRegisterer = fragmentRegisterer;
		this.threads = threads;
	}

	public void monitor() throws Exception {
		long numberOfCrds = 0;
		long numberOfFragments = 0;

		while (true) {

			try {
				Thread.sleep(Constants.MONITORING_INTERVAL);

				if (detectedCrds.size() >= maxElementsCount
						|| detectedFragments.size() >= maxElementsCount) {
					synchronized (detectedCrds) {
						synchronized (detectedFragments) {
							final Set<DBCrdInfo> currentCrds = new TreeSet<DBCrdInfo>();
							currentCrds.addAll(detectedCrds.values());
							crdRegisterer.register(currentCrds);
							logger.info(currentCrds.size()
									+ " CRDs have been registered into db");
							numberOfCrds += currentCrds.size();

							for (final DBCrdInfo crd : currentCrds) {
								detectedCrds.remove(crd.getId());
							}

							final Set<DBCodeFragmentInfo> currentFragments = new TreeSet<DBCodeFragmentInfo>();
							currentFragments.addAll(detectedFragments.values());
							fragmentRegisterer.register(currentFragments);
							logger.info(currentFragments.size()
									+ " fragments have been registered into db");
							numberOfFragments += currentFragments.size();

							for (final DBCodeFragmentInfo fragment : currentFragments) {
								detectedFragments.remove(fragment.getId());
							}
						}
					}
				}

			} catch (Exception e) {
				eLogger.warn(
						"something is wrong in CodeFragmentDetectingThreadMonitor\n",
						e);
				if (e instanceof SQLException) {
					final SQLException se = (SQLException) e;
					eLogger.warn("error code: " + se.getErrorCode());

					SQLException ne = null;
					while ((ne = se.getNextException()) != null) {
						eLogger.warn("next exception: ", ne);
					}
				}
			}

			// break this loop if all the other threads have died
			boolean allThreadDead = true;
			for (final Thread thread : threads) {
				if (thread.isAlive()) {
					allThreadDead = false;
					break;
				}
			}

			if (allThreadDead) {
				break;
			}

		}

		logger.info("all threads have finished their work");
		logger.info("registering all the remaining elements into db ");
		crdRegisterer.register(detectedCrds.values());
		fragmentRegisterer.register(detectedFragments.values());

		numberOfCrds += detectedCrds.size();
		numberOfFragments += detectedFragments.size();

		logger.info("the numbers of detected elements are ... ");
		logger.info("CRD: " + numberOfCrds);
		logger.info("Fragment: " + numberOfFragments);
	}

}
