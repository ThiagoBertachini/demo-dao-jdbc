package application;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import db.DB;
import db.DbIntegrityException;
import model.Department;

public class Program {

	public static void main(String[] args) {

		Department dep = new Department(1, "RH");
		
		System.out.println(dep);
	}
}
