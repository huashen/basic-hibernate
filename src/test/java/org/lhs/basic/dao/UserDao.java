package org.lhs.basic.dao;

import org.lhs.basic.dao.BaseDao;
import org.lhs.basic.model.User;
import org.springframework.stereotype.Repository;

@Repository("userDao")
public class UserDao extends BaseDao<User> implements IUserDao 
{

}
