.PHONY: build-all
build-all: ## Build debug version 
	@echo "Building debug version"
	cargo build 
	cp ./broker/yedmq.toml ./target/debug/yedmq.toml
	mkdir ./target/debug/plugins
	mkdir ./target/debug/plugins/acl_file
	mkdir ./target/debug/plugins/acl_mysql
	mkdir ./target/debug/plugins/acl_redis
	mkdir ./target/debug/plugins/acl_postgresql
	cp ./plugins/acl_mysql/acl_mysql.toml ./target/debug/plugins/acl_mysql/acl_mysql.toml
	cp ./plugins/acl_redis/acl_redis.toml ./target/debug/plugins/acl_redis/acl_redis.toml
	cp ./plugins/acl_postgresql/acl_postgresql.toml ./target/debug/plugins/acl_postgresql/acl_postgresql.toml
	cp ./target/debug/libyedmq_plugins_acl_mysql.so ./target/debug/plugins/acl_mysql/libyedmq_plugins_acl_mysql.so
	cp ./target/debug/libyedmq_plugins_acl_redis.so ./target/debug/plugins/acl_redis/libyedmq_plugins_acl_redis.so
	cp ./target/debug/libyedmq_plugins_acl_postgresql.so ./target/debug/plugins/acl_postgresql/libyedmq_plugins_acl_postgresql.so
	cp ./target/debug/libyedmq_plugins_acl_file.so ./target/debug/plugins/acl_file/libyedmq_plugins_acl_file.so
	cp ./plugins/acl_file/plugin.toml ./target/debug/plugins/acl_file/plugin.toml
	cp ./plugins/acl_mysql/plugin.toml ./target/debug/plugins/acl_mysql/plugin.toml
	cp ./plugins/acl_redis/plugin.toml ./target/debug/plugins/acl_redis/plugin.toml
	cp ./plugins/acl_postgresql/plugin.toml ./target/debug/plugins/acl_postgresql/plugin.toml

build-all-release: ## Build release version
	@echo "Building release version"
	cargo build --release
	cp ./broker/yedmq.toml ./target/release/yedmq.toml
	mkdir ./target/release/plugins
	mkdir ./target/release/plugins/acl_file
	mkdir ./target/release/plugins/acl_mysql
	mkdir ./target/release/plugins/acl_redis
	mkdir ./target/release/plugins/acl_postgresql
	cp ./plugins/acl_mysql/acl_mysql.toml ./target/release/plugins/acl_mysql/acl_mysql.toml
	cp ./plugins/acl_redis/acl_redis.toml ./target/release/plugins/acl_redis/acl_redis.toml
	cp ./plugins/acl_postgresql/acl_postgresql.toml ./target/release/plugins/acl_postgresql/acl_postgresql.toml
	cp ./target/release/libyedmq_plugins_acl_mysql.so ./target/release/plugins/acl_mysql/libyedmq_plugins_acl_mysql.so
	cp ./target/release/libyedmq_plugins_acl_redis.so ./target/release/plugins/acl_redis/libyedmq_plugins_acl_redis.so
	cp ./target/release/libyedmq_plugins_acl_postgresql.so ./target/release/plugins/acl_postgresql/libyedmq_plugins_acl_postgresql.so
	cp ./target/release/libyedmq_plugins_acl_file.so ./target/release/plugins/acl_file/libyedmq_plugins_acl_file.so
	cp ./plugins/acl_file/plugin.toml ./target/release/plugins/acl_file/plugin.toml
	cp ./plugins/acl_mysql/plugin.toml ./target/release/plugins/acl_mysql/plugin.toml
	cp ./plugins/acl_redis/plugin.toml ./target/release/plugins/acl_redis/plugin.toml
	cp ./plugins/acl_postgresql/plugin.toml ./target/release/plugins/acl_postgresql/plugin.toml
