package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DbException;
import model.dao.DepartmentDAO;
import model.dao.SellerDAO;
import model.entities.Department;
import model.entities.Seller;

public class DeparmentDaoJDBC implements DepartmentDAO{
	
	private Connection conn;
	
	public DeparmentDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void deleteById(Integer id) {
		PreparedStatement ps = null;
		
		try {
			ps = conn.prepareStatement("DELETE FROM seller WHERE Id = ?");
			
			ps.setInt(1, id);
			
			ps.execute();
		}catch (SQLException e) {
			throw new DbException(e.getMessage());
		}finally {
			DB.closeStatement(ps);
		}
		
	}
	
	@Override
	public List<Department> findAll() {
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try {
			ps = conn.prepareStatement(
					"SELECT department.* "
					+ "FROM department "
					+ "ORDER BY Name");
			
			rs = ps.executeQuery();
			
			List<Department> depList = new ArrayList<>();
	
			while(rs.next()) {
				
				Department dept = instanciateDept(rs);
				
				depList.add(dept);
			}
				return depList;
		}catch (SQLException e) {
			throw new DbException(e.getMessage());
		}finally {
			DB.closeStatement(ps);
			DB.closeResultSet(rs);
		}
	}

	@Override
	public void insert(Department department) {
		PreparedStatement ps = null;
		
		try {
			ps = conn.prepareStatement(
					"INSERT INTO department "
					+ "(Name) VALUES (?)",
					Statement.RETURN_GENERATED_KEYS);
			
			ps.setString(1, department.getName());
			
			int rowsAffected = ps.executeUpdate();
		
			if(rowsAffected > 0) {
				ResultSet rs = ps.getGeneratedKeys();
				if(rs.next()) {
					department.setId(rs.getInt(1));
				}
				DB.closeResultSet(rs);
			}
			else {
				throw new DbException("Unecpected error, no rows affected");
			}
		}catch (SQLException e) {
			throw new DbException(e.getMessage());
		}finally {
			DB.closeStatement(ps);
		}
	}

	@Override
	public void update(Department department) {
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(
					"UPDATE department SET Name = ?) "
					+ "WHERE Id = ?");
			
			ps.setString(1, department.getName());
			ps.setInt(2, department.getId());
			
			ps.executeUpdate();
		}catch (SQLException e) {
			throw new DbException(e.getMessage());
		}finally {
			DB.closeStatement(ps);
		}
	}

	@Override
	public Department findById(Integer id) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(
					"SELECT department.* "
					+ "FROM department "
					+ "WHERE department.Id = ?");
			
			ps.setInt(1, id);
			
			rs = ps.executeQuery();
			if(rs.next()) {
				Department dept = instanciateDept(rs);
				return dept;
			}
			return null;
		}catch (SQLException e) {
			throw new DbException(e.getMessage());
		}finally {
			DB.closeStatement(ps);
			DB.closeResultSet(rs);
		}
	}
	
	private Department instanciateDept(ResultSet rs) throws SQLException {
		 Department dept = new Department();
			dept.setId(rs.getInt("DepartmentId"));
			dept.setName(rs.getString("DepName"));
		return dept;
	}
}