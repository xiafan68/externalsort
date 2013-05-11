/**
 * @author chengchengyu
 */
package index;

import java.io.File;
import java.util.Comparator;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.PreloadConfig;

/**
 * DbEnv is used to simply the configuration to set up Berkeley DB. It sets up
 * default value for some parameters while giving some flexibility for the user
 * to customize some parameters. 
 */
public class DbEnv {

	private Environment env;
	private Database nodeDb;
	private TupleBinding nodeBinding;
	private EnvironmentConfig myEnvConfig = new EnvironmentConfig();

	public EnvironmentConfig getMyEnvConfig() {
		return myEnvConfig;
	}

	public void setMyEnvConfig(EnvironmentConfig myEnvConfig) {
		this.myEnvConfig = myEnvConfig;
	}

	public DatabaseConfig getMyDbConfig() {
		return myDbConfig;
	}

	public void setMyDbConfig(DatabaseConfig myDbConfig) {
		this.myDbConfig = myDbConfig;
	}

	private DatabaseConfig myDbConfig = new DatabaseConfig();
	private EnvironmentMutableConfig mutableConfig = new EnvironmentMutableConfig();

	File envHome;
	Class<Comparator<byte[]>> nodeCompClass;
	
	public DbEnv(File envHome_, TupleBinding binding_, Class<Comparator<byte[]>> nodeCompClass_) {
		envHome = envHome_;
		nodeBinding = binding_;
		nodeCompClass = nodeCompClass_;
	}

	public void setup(boolean readOnly,
			boolean duplicatesAllowed, int cacheSize) throws DatabaseException {
		if (!envHome.exists()) {
			envHome.mkdir();
		}

		// If the environment is read-only, then
		// make the databases read-only too.
		myEnvConfig.setReadOnly(readOnly);
		myDbConfig.setReadOnly(readOnly);

		// If the environment is opened for write, then we want to be
		// able to create the environment and databases if
		// they do not exist.
		myEnvConfig.setAllowCreate(!readOnly);
		myDbConfig.setAllowCreate(!readOnly);

		// Allow transactions if we are writing to the database
		/*
		 * myEnvConfig.setTransactional(!readOnly);
		 * myDbConfig.setTransactional(!readOnly);
		 */

		myEnvConfig.setTransactional(false);
		myDbConfig.setTransactional(false);

		myDbConfig.setDeferredWrite(true);
		myDbConfig.setDuplicateComparator(nodeCompClass);
		// myDbConfig.setCacheMode(CacheMode.DYNAMIC);
		//mutableConfig.setCacheSize(cacheSize);
		//mutableConfig.setCacheSize(1024*1024*1024);
		mutableConfig.setCachePercent(20);
		// myEnvConfig.setSortedDuplicates(true);
		myDbConfig.setSortedDuplicates(duplicatesAllowed);

		// Open the environment
		env = new Environment(envHome, myEnvConfig);
		env.setMutableConfig(mutableConfig);
		// Now open, or create and open, our databases
		nodeDb = env.openDatabase(null, "NodesDB", myDbConfig);
		//PreloadConfig preloadConfig = new PreloadConfig();
		// preloadConfig.setMaxBytes(1024*1024*128).setMaxMillisecs(1000*60);
		// preloadConfig.setMaxBytes(cacheSize * 2).setMaxMillisecs(1000*60);
		//nodeDb.preload(preloadConfig);
	}

	public void flush() {
		nodeDb.sync();
	}

	public TupleBinding getNodeBinding() {
		return nodeBinding;
	}

	public void setNodeBinding(TupleBinding nodeBinding) {
		this.nodeBinding = nodeBinding;
	}

	public Environment getEnv() {
		return env;
	}

	public void setEnv(Environment env) {
		this.env = env;
	}

	public Database getNodeDb() {
		return nodeDb;
	}

	public void setNodeDb(Database nodeDb) {
		this.nodeDb = nodeDb;
	}

	public void close() {
		if (env != null) {
			try {
				nodeDb.close();
				env.cleanLog();
				env.close();
			} catch (DatabaseException dbe) {
				System.err.println("Error closing MyDbEnv: " + dbe.toString());
				System.exit(-1);
			}
		}
	}

}