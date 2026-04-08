.PHONY: build-all build-all-release

PLUGIN_NAME := acl_file
PLUGIN_SOURCE_DIR := ./example_plugins/$(PLUGIN_NAME)

build-all: ## Build debug binaries and stage the bundled example plugin
	cargo build -p yedmq -p $(PLUGIN_NAME)
	mkdir -p ./target/debug/plugins/$(PLUGIN_NAME)
	cp ./yedmq.toml.example ./target/debug/yedmq.toml
	cp $(PLUGIN_SOURCE_DIR)/acl.json ./target/debug/plugins/$(PLUGIN_NAME)/acl.json
	printf '%s\n' \
		'[plugin]' \
		'name = "$(PLUGIN_NAME)"' \
		'version = "0.1.0"' \
		'description = "File-based ACL plugin example for YedMQ"' \
		'author = "YedMQ"' \
		'license = "Apache-2.0"' \
		'homepage = "https://www.yedmq.com"' \
		'repository = "https://github.com/designershao/YedMQ"' \
		'' \
		'[runtime]' \
		'type = "process"' \
		'executable = "../../$(PLUGIN_NAME)"' \
		'args = ["--acl-file", "./acl.json"]' \
		'env = {}' \
		'working_dir = "."' \
		'timeout_secs = 12' \
		> ./target/debug/plugins/$(PLUGIN_NAME)/plugin.toml

build-all-release: ## Build release binaries and stage the bundled example plugin
	cargo build --release -p yedmq -p $(PLUGIN_NAME)
	mkdir -p ./target/release/plugins/$(PLUGIN_NAME)
	cp ./yedmq.toml.example ./target/release/yedmq.toml
	cp $(PLUGIN_SOURCE_DIR)/acl.json ./target/release/plugins/$(PLUGIN_NAME)/acl.json
	printf '%s\n' \
		'[plugin]' \
		'name = "$(PLUGIN_NAME)"' \
		'version = "0.1.0"' \
		'description = "File-based ACL plugin example for YedMQ"' \
		'author = "YedMQ"' \
		'license = "Apache-2.0"' \
		'homepage = "https://www.yedmq.com"' \
		'repository = "https://github.com/designershao/YedMQ"' \
		'' \
		'[runtime]' \
		'type = "process"' \
		'executable = "../../$(PLUGIN_NAME)"' \
		'args = ["--acl-file", "./acl.json"]' \
		'env = {}' \
		'working_dir = "."' \
		'timeout_secs = 12' \
		> ./target/release/plugins/$(PLUGIN_NAME)/plugin.toml
