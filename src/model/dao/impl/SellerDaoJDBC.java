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
import model.dao.SellerDAO;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDAO{
	
	private Connection conn;
	
	public SellerDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Seller seller) {
		PreparedStatement ps = null;
		
		try {
			ps = conn.prepareStatement(
					"INSERT INTO seller "
					+ "(Name, Email, BirthDate, BaseSalary, DepartmentId) "
					+ "VALUES "
					+ "(?, ?, ?, ?, ?)",
					Statement.RETURN_GENERATED_KEYS);
			
			ps.setString(1, seller.getName());
			ps.setString(2, seller.getEmail());
			ps.setDate(3, new java.sql.Date(seller.getBirthdate().getTime()));
			ps.setDouble(4, seller.getBaseSalary());
			ps.setInt(5, seller.getDepartment().getId());
			
			int rowsAffected = ps.executeUpdate();
		
			if(rowsAffected > 0) {
				ResultSet rs = ps.getGeneratedKeys();
				if(rs.next()) {
					seller.setId(rs.getInt(1));
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
	public void update(Seller seller) {
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(
					"UPDATE seller "
					+ "SET Name = ?, Email = ?, BirthDate = ?, BaseSalary = ?, DepartmentId = ?) "
					+ "WHERE Id = ?");
			
			ps.setString(1, seller.getName());
			ps.setString(2, seller.getEmail());
			ps.setDate(3, new java.sql.Date(seller.getBirthdate().getTime()));
			ps.setDouble(4, seller.getBaseSalary());
			ps.setInt(5, seller.getDepartment().getId());
			ps.setInt(6, seller.getId());
			
			ps.executeUpdate();
		}catch (SQLException e) {
			throw new DbException(e.getMessage());
		}finally {
			DB.closeStatement(ps);
		}
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
	public Seller findById(Integer id) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName "
					+ "FROM seller INNER JOIN department "
					+ "ON seller.DepartmentId = department.Id "
					+ "WHERE seller.Id = ?");
			ps.setInt(1, id);
			
			rs = ps.executeQuery();
			if(rs.next()) {
				Department dept = instanciateDept(rs);
				Seller seller = instanciateSeller(rs, dept);
				return seller;
			}
			return null;
		}catch (SQLException e) {
			throw new DbException(e.getMessage());
		}finally {
			DB.closeStatement(ps);
			DB.closeResultSet(rs);
		}
	}

	@Override
	public List<Seller> findAll() {
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try {
			ps = conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName "
					+ "FROM seller INNER JOIN department "
					+ "ON seller.DepartmentId = department.Id "
					+ "ORDER BY Name");
			
			rs = ps.executeQuery();
			
			Map<Integer, Department> depMap = new HashMap<>();
			List<Seller> sellerList = new ArrayList<>();
	
			while(rs.next()) {
				
				Department dept = depMap.get(rs.getInt("DepartmentId"));
				
				if(dept == null) {
					dept = instanciateDept(rs);	
					depMap.put(rs.getInt("DepartmentId"), dept);
				}
				
				Seller seller = instanciateSeller(rs, dept);
				
				sellerList.add(seller);
			}
				return sellerList;
		}catch (SQLException e) {
			throw new DbException(e.getMessage());
		}finally {
			DB.closeStatement(ps);
			DB.closeResultSet(rs);
		}
	}

	@Override
	public List<Seller> findByDepartment(Department department) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try {
			ps = conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName "
					+ "FROM seller INNER JOIN department "
					+ "ON seller.DepartmentId = department.Id "
					+ "WHERE department.id = ? "
					+ "ORDER BY Name");
			
			ps.setInt(1, department.getId());
			
			rs = ps.executeQuery();
			
			Map<Integer, Department> depMap = new HashMap<>();
			List<Seller> sellerList = new ArrayList<>();
	
			while(rs.next()) {
				
				Department dept = depMap.get(rs.getInt("DepartmentId"));
				
				if(dept == null) {
					dept = instanciateDept(rs);	
					depMap.put(rs.getInt("DepartmentId"), dept);
				}
				
				Seller seller = instanciateSeller(rs, dept);
				
				sellerList.add(seller);
			}
				return sellerList;
		}catch (SQLException e) {
			throw new DbException(e.getMessage());
		}finally {
			DB.closeStatement(ps);
			DB.closeResultSet(rs);
		}
	}

	private Seller instanciateSeller(ResultSet rs, Department dept) throws SQLException {
		Seller seller = new Seller();
		seller.setId(rs.getInt("Id"));
		seller.setName(rs.getString("Name"));
		seller.setEmail(rs.getString("Email"));
		seller.setBaseSalary(rs.getDouble("BaseSalary"));
		seller.setBirthdate(rs.getDate("BirthDate"));
		seller.setDepartment(dept);
		return seller;
	}

	private Department instanciateDept(ResultSet rs) throws SQLException {
		 Department dept = new Department();
			dept.setId(rs.getInt("DepartmentId"));
			dept.setName(rs.getString("DepName"));
		return dept;
	}
	
}