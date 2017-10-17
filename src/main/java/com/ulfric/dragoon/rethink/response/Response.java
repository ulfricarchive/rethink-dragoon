package com.ulfric.dragoon.rethink.response;

import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.UUID;

public class Response {

	private Integer deleted;
	private Integer inserted;
	private Integer unchanged;
	private Integer replaced;
	private Integer errors;
	private Integer skipped;
	private Integer synced;
	@SerializedName("dbs_created")
	private Integer databasesCreated;
	@SerializedName("tables_created")
	private Integer tablesCreated;
	@SerializedName("generated_keys")
	private List<UUID> generatedKeys;

	public Integer getDeleted() {
		return deleted;
	}

	public void setDeleted(Integer deleted) {
		this.deleted = deleted;
	}

	public Integer getInserted() {
		return inserted;
	}

	public void setInserted(Integer inserted) {
		this.inserted = inserted;
	}

	public Integer getUnchanged() {
		return unchanged;
	}

	public void setUnchanged(Integer unchanged) {
		this.unchanged = unchanged;
	}

	public Integer getReplaced() {
		return replaced;
	}

	public void setReplaced(Integer replaced) {
		this.replaced = replaced;
	}

	public Integer getErrors() {
		return errors;
	}

	public void setErrors(Integer errors) {
		this.errors = errors;
	}

	public Integer getSkipped() {
		return skipped;
	}

	public void setSkipped(Integer skipped) {
		this.skipped = skipped;
	}

	public Integer getSynced() {
		return synced;
	}

	public void setSynced(Integer synced) {
		this.synced = synced;
	}

	public Integer getDatabasesCreated() {
		return databasesCreated;
	}

	public void setDatabasesCreated(Integer databasesCreated) {
		this.databasesCreated = databasesCreated;
	}

	public Integer getTablesCreated() {
		return tablesCreated;
	}

	public void setTablesCreated(Integer tablesCreated) {
		this.tablesCreated = tablesCreated;
	}

	public List<UUID> getGeneratedKeys() {
		return generatedKeys;
	}

	public void setGeneratedKeys(List<UUID> generatedKeys) {
		this.generatedKeys = generatedKeys;
	}

}
