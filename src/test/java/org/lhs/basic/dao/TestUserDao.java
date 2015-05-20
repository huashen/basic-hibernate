package org.lhs.basic.dao;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import junit.framework.Assert;

import org.dbunit.DatabaseUnitException;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.operation.DatabaseOperation;
import org.hibernate.ObjectNotFoundException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.lhs.basic.model.Pager;
import org.lhs.basic.model.SystemContext;
import org.lhs.basic.model.User;
import org.lhs.basic.test.util.AbstractDbUnitTestCase;
import org.lhs.basic.test.util.EntitiesHelper;
import org.springframework.orm.hibernate4.SessionHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("/beans.xml")
public class TestUserDao extends AbstractDbUnitTestCase
{

	@Inject
	private SessionFactory sessionFactory;
	
	@Inject
	private IUserDao userDao;
	
	@Before
	public void setUp() throws DataSetException, SQLException, IOException 
	{
		Session s = sessionFactory.openSession();
		TransactionSynchronizationManager.bindResource(sessionFactory, new SessionHolder(s));
		this.backupAllTable();
	}
	
	
	@Test
	public void testLoad() throws DatabaseUnitException, SQLException
	{
		IDataSet ds = createDateSet("t_user");
		DatabaseOperation.CLEAN_INSERT.execute(dbunitCon, ds);
		User u = userDao.load(1);
		EntitiesHelper.assertUser(u);
	}
	
	@Test
	public void testDelete() throws DatabaseUnitException, SQLException
	{
		IDataSet ds = createDateSet("t_user");
		DatabaseOperation.CLEAN_INSERT.execute(dbunitCon, ds);
		userDao.delete(1);
		User tu = userDao.load(1);
		Assert.assertNull(tu);
	}
	
	@Test
	public void testListByArgs() throws DatabaseUnitException, SQLException
	{
		IDataSet ds = createDateSet("t_user");
		DatabaseOperation.CLEAN_INSERT.execute(dbunitCon, ds);
		SystemContext.setOrder("desc");
		SystemContext.setSort("id");
		List<User> expected = userDao.list("from User where id>? and id<?", new Object[]{1,4});
		List<User> actuals = Arrays.asList(new User(3,"admin3"),new User(2,"admin2"));
		EntitiesHelper.assertUsers(expected, actuals);
	}
	
	@Test
	public void testListByArgsAndAlias() throws DatabaseUnitException, SQLException
	{
		IDataSet ds = createDateSet("t_user");
		DatabaseOperation.CLEAN_INSERT.execute(dbunitCon, ds);
		SystemContext.setOrder("asc");
		SystemContext.setSort("id");
		Map<String,Object> alias = new HashMap<String,Object>();
		alias.put("ids", Arrays.asList(1,2,3,5,6,7,8,9,10));
		List<User> expected = userDao.list("from User where id >? and id<? and id in(:ids)", new Object[]{1,5},alias);
		List<User> actuals = Arrays.asList(new User(2,"admin2"),new User(3,"admin3"));
		Assert.assertNotNull(expected);
		EntitiesHelper.assertUsers(expected, actuals);		
	}
	
	@Test
	public void testFindByArgs() throws DatabaseUnitException, SQLException
	{
		IDataSet ds = createDateSet("t_user");
		DatabaseOperation.CLEAN_INSERT.execute(dbunitCon, ds);
		SystemContext.setOrder("desc");
		SystemContext.setSort("id");
		SystemContext.setPageOffset(0);
		SystemContext.setPageSize(3);
		Pager<User> expected = userDao.find("from User where id>=? and id<=?", new Object[]{1,10});
		List<User> actuals = Arrays.asList(new User(10,"admin10"),new User(9,"admin9"),new User(8,"admin8"));
		Assert.assertNotNull(expected);
		Assert.assertTrue(expected.getTotal() == 10);
		Assert.assertTrue(expected.getOffset() == 0);
		Assert.assertTrue(expected.getSize() == 3);
		EntitiesHelper.assertUsers(expected.getDatas(), actuals);
	}
	
	@Test
	public void testFindByArgsAndAlias() throws DatabaseUnitException, SQLException
	{
		IDataSet ds = createDateSet("t_user");
		DatabaseOperation.CLEAN_INSERT.execute(dbunitCon, ds);
		SystemContext.removeOrder();
		SystemContext.removeSort();
		SystemContext.setPageOffset(0);
		SystemContext.setPageSize(3);
		Map<String,Object> alias = new HashMap<String,Object>();
		alias.put("ids", Arrays.asList(new Object[]{1,2,4,5,6,7,8,10}));
		Pager<User> expected = userDao.find("from User where id>=? and id<=? and id in (:ids)", new Object[]{1,10},alias);
		List<User> actuals = Arrays.asList(new User(1,"admin1"),new User(2,"admin2"),new User(4,"admin4"));
		Assert.assertNotNull(expected);
		Assert.assertTrue(expected.getTotal() == 8);
		Assert.assertTrue(expected.getOffset() == 0);
		Assert.assertTrue(expected.getSize() == 3);
		EntitiesHelper.assertUsers(expected.getDatas(), actuals);
	}
	
	@Test
	public void testFindSQLByArgs() throws DatabaseUnitException, SQLException 
	{
		IDataSet ds = createDateSet("t_user");
		DatabaseOperation.CLEAN_INSERT.execute(dbunitCon, ds);
		SystemContext.setOrder("desc");
		SystemContext.setSort("id");
		SystemContext.setPageOffset(0);
		SystemContext.setPageSize(3);
		Pager<User> expected = userDao.findBySql("select * from t_user where id>=? and id<=?", new Object[]{1,10},User.class,true);
		List<User> actuals = Arrays.asList(new User(10,"admin10"),new User(9,"admin9"),new User(8,"admin8"));
		Assert.assertNotNull(expected);
		Assert.assertTrue(expected.getTotal() == 10);
		Assert.assertTrue(expected.getOffset() == 0);
		Assert.assertTrue(expected.getSize() == 3);
		EntitiesHelper.assertUsers(expected.getDatas(), actuals);
	}
	
	@After
	public void tearDown() throws FileNotFoundException, DatabaseUnitException, SQLException
	{
		SessionHolder holder = (SessionHolder) TransactionSynchronizationManager.getResource(sessionFactory);
		Session s = holder.getSession(); 
		s.flush();
		TransactionSynchronizationManager.unbindResource(sessionFactory);
		this.resumeTable();
	}
}
