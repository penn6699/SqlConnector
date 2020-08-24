using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

using System.Data;
using System.Data.SqlClient;

/// <summary>
/// 数据库连接器
/// </summary>
public class SqlServerConnector : IDisposable
{

    #region 字段

    /// <summary>
    /// 数据库连接
    /// </summary>
    private SqlConnection conn = null;
    
    #endregion

    #region 属性

    /// <summary>
    /// 数据库连接
    /// </summary>
    public SqlConnection SqlConnection {
        get {
            return conn;
        }
    }


    #endregion
    
    #region 构造函数

    /// <summary>
    /// 构造函数
    /// </summary>
    public SqlServerConnector()
    {
        string ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["defaultDB"].ConnectionString;
        conn = new SqlConnection(ConnectionString);
        conn.Open();
    }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="ConnectionString">数据库连接字符串</param>
    public SqlServerConnector(string ConnectionString)
    {
        conn = new SqlConnection(ConnectionString);
        conn.Open();
    }

    #endregion
    
    #region SqlConnection

    /// <summary>
    /// 
    /// </summary>
    public void CloseConnection()
    {
        conn.Close();
    }

    /// <summary>
    /// 
    /// </summary>
    public void OpenConnection()
    {
        conn.Open();
    }

    #endregion

    #region SqlCommand

    /// <summary>
    /// 创建数据库命令
    /// </summary>
    /// <param name="SQL">sql语句</param>
    /// <param name="pars">sql参数</param>
    /// <param name="CommandTimeout">超时时间。以秒为单位</param>
    /// <returns></returns>
    public SqlCommand CreateSqlCommand(string SQL, SqlParameter[] pars, CommandType cmdType, int CommandTimeout = 60)
    {
        SqlCommand _SqlCommand = new SqlCommand();
        _SqlCommand.Connection = conn;
        _SqlCommand.CommandType = cmdType;

        if (!string.IsNullOrEmpty(SQL))
        {
            _SqlCommand.CommandText = SQL;
        }
        //以秒为单位
        _SqlCommand.CommandTimeout = CommandTimeout;

        _SqlCommand.Parameters.Clear();
        if (pars != null && pars.Length > 0)
        {
            _SqlCommand.Parameters.AddRange(pars);
        }
        return _SqlCommand;
    }

    /// <summary>
    /// 创建数据库命令
    /// </summary>
    /// <param name="SQL">sql语句</param>
    /// <param name="CommandTimeout">超时时间。以秒为单位</param>
    /// <returns></returns>
    public SqlCommand CreateSqlCommand(string SQL, CommandType cmdType, int CommandTimeout = 60)
    {
        return CreateSqlCommand(SQL, null, cmdType, CommandTimeout);
    }

    /// <summary>
    /// 创建数据库命令
    /// </summary>
    /// <param name="CommandTimeout">超时时间。以秒为单位</param>
    /// <returns></returns>
    public SqlCommand CreateSqlCommand(CommandType cmdType,int CommandTimeout = 60)
    {
        return CreateSqlCommand(null, null, cmdType, CommandTimeout);
    }

    

    #endregion

    #region 执行 SqlCommand 的ExecuteNonQuery、ExecuteReader、ExecuteScalar、ExecuteXmlReader方法

    /// <summary>
    /// 执行SQL语句或存储过程
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">参数</param>
    /// <param name="cmdType">命令类型</param>
    /// <returns>返回受影响的行数</returns>
    /// <returns></returns>
    public int ExecuteNonQuery(string sql, SqlParameter[] parameters, CommandType cmdType)
    {
        SqlCommand _SqlCommand = CreateSqlCommand(sql, parameters, cmdType);
        try
        {
            return _SqlCommand.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            throw ex;
        }
        finally
        {
            _SqlCommand.Dispose();
        }
    }

    /// <summary>
    /// 执行SQL语句
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">参数</param>
    /// <returns>返回受影响的行数</returns>
    public int ExecuteNonQuery(string sql, SqlParameter[] parameters)
    {
        return ExecuteNonQuery(sql, parameters, CommandType.Text);
    }

    /// <summary>
    /// 执行SQL语句
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <returns>返回受影响的行数</returns>
    public int ExecuteNonQuery(string sql)
    {
        return ExecuteNonQuery(sql, null, CommandType.Text);
    }
    
    /// <summary>
    /// 执行SQL语句或存储过程
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">参数</param>
    /// <param name="cmdType">命令类型</param>
    /// <returns></returns>
    public SqlDataReader ExecuteReader(string sql, SqlParameter[] parameters, CommandType cmdType)
    {
        SqlCommand _SqlCommand = CreateSqlCommand(sql, parameters, cmdType);
        try
        {
            return _SqlCommand.ExecuteReader();
        }
        catch (Exception ex)
        {
            throw ex;
        }
        finally
        {
            _SqlCommand.Dispose();
        }
    }

    /// <summary>
    /// 执行SQL语句
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">参数。类型为Dictionary字典（参数名带不带@都可以）或 SqlParameter[] </param>
    /// <returns></returns>
    public SqlDataReader ExecuteReader(string sql, SqlParameter[] parameters)
    {
        return ExecuteReader(sql, parameters, CommandType.Text);
    }

    /// <summary>
    /// 执行SQL语句
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <returns></returns>
    public SqlDataReader ExecuteReader(string sql)
    {
        return ExecuteReader(sql, null, CommandType.Text);
    }

    /// <summary>
    /// 执行SQL语句或存储过程
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">参数</param>
    /// <param name="cmdType">命令类型</param>
    /// <returns>返回查询所返回的结果集中的第一行第一列</returns>
    public object ExecuteScalar(string sql, SqlParameter[] parameters, CommandType cmdType)
    {
        SqlCommand _SqlCommand = CreateSqlCommand(sql, parameters, cmdType);
        try
        {
            return _SqlCommand.ExecuteScalar();
        }
        catch (Exception ex)
        {
            throw ex;
        }
        finally
        {
            _SqlCommand.Dispose();
        }
    }

    /// <summary>
    /// 执行SQL语句
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">参数</param>
    /// <returns>返回查询所返回的结果集中的第一行第一列</returns>
    public object ExecuteScalar(string sql, SqlParameter[] parameters)
    {
        return ExecuteScalar(sql, parameters,CommandType.Text);
    }

    /// <summary>
    /// 执行SQL语句
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <returns>返回查询所返回的结果集中的第一行第一列</returns>
    public object ExecuteScalar(string sql)
    {
        return ExecuteScalar(sql, null, CommandType.Text);
    }

    #endregion

    #region 执行 ExecuteDataTable、ExecuteDataSet


    /// <summary>
    /// 执行SQL语句或存储过程
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">参数</param>
    /// <param name="cmdType">执行类型</param>
    /// <returns></returns>
    public DataTable ExecuteDataTable(string sql, SqlParameter[] parameters, CommandType cmdType)
    {
        SqlCommand _SqlCommand = CreateSqlCommand(sql, parameters, cmdType);
        try
        {
            using (DataTable dt = new DataTable())
            {
                SqlDataAdapter sda = new SqlDataAdapter(_SqlCommand);
                sda.Fill(dt);

                sda.Dispose();

                return dt;
            }

        }
        catch (Exception e)
        {
            throw e;
        }
        finally
        {
            _SqlCommand.Dispose();
        }
    }

    /// <summary>
    /// 执行SQL语句
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">参数</param>
    public DataTable ExecuteDataTable(string sql, SqlParameter[] parameters)
    {
        return ExecuteDataTable(sql, parameters,CommandType.Text);
    }

    /// <summary>
    /// 执行SQL语句
    /// </summary>
    /// <param name="sql">SQL语句</param>
    public DataTable ExecuteDataTable(string sql)
    {
        return ExecuteDataTable(sql, null, CommandType.Text);
    }

    /// <summary>
    /// 执行SQL语句或存储过程
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">参数</param>
    /// <param name="cmdType">执行类型</param>
    /// <returns></returns>
    public DataSet ExecuteDataSet(string sql, SqlParameter[] parameters, CommandType cmdType)
    {
        SqlCommand _SqlCommand = CreateSqlCommand(sql,parameters, cmdType);
        try
        {
            SqlDataAdapter sda = new SqlDataAdapter(_SqlCommand);
            using (DataSet ds = new DataSet())
            {
                sda.Fill(ds);
                sda.Dispose();

                return ds;
            }

        }
        catch (Exception e)
        {
            throw e;
        }
        finally
        {
            _SqlCommand.Dispose();
        }
    }

    /// <summary>
    /// 执行SQL语句或存储过程
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">参数</param>
    /// <returns></returns>
    public DataSet ExecuteDataSet(string sql, SqlParameter[] parameters)
    {
        return ExecuteDataSet(sql, parameters,CommandType.Text);
    }

    /// <summary>
    /// 执行SQL语句
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <returns></returns>
    public DataSet ExecuteDataSet(string sql)
    {
        return ExecuteDataSet(sql, null, CommandType.Text);
    }


    #endregion
    
    #region IDisposable Support

    private bool disposedValue = false; // 要检测冗余调用

    protected virtual void Dispose(bool disposing)
    {
        if (!disposedValue)
        {
            if (disposing)
            {
                // TODO: 释放托管状态(托管对象)。

                if (conn != null) {
                    conn.Close();
                    conn.Dispose();
                }
                
            }

            // TODO: 释放未托管的资源(未托管的对象)并在以下内容中替代终结器。
            // TODO: 将大型字段设置为 null。

            disposedValue = true;
        }
    }

    // TODO: 仅当以上 Dispose(bool disposing) 拥有用于释放未托管资源的代码时才替代终结器。
    // ~SqlConnector() {
    //   // 请勿更改此代码。将清理代码放入以上 Dispose(bool disposing) 中。
    //   Dispose(false);
    // }

    // 添加此代码以正确实现可处置模式。
    void IDisposable.Dispose()
    {
        // 请勿更改此代码。将清理代码放入以上 Dispose(bool disposing) 中。
        Dispose(true);
        // TODO: 如果在以上内容中替代了终结器，则取消注释以下行。
        // GC.SuppressFinalize(this);
    }
    #endregion


}