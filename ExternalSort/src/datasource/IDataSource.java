package datasource;

import java.io.IOException;


/**
 * 从数据来源读取数据,目前只处理类型为string的数据
 * @author xiafan
 *
 */
public interface IDataSource {
	public boolean hasNext();
	public IRecord next();
	public void close() throws IOException;
}
