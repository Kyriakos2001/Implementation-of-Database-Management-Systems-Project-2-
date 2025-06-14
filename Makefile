# Define targets
build: bplus
debug:
	@echo "Compile bplus_main ..."
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bp_main.c ./src/record.c ./src/bp_file.c ./src/bp_datanode.c ./src/bp_indexnode.c -lbf -g -o ./build/bplus_main -O2 -lm

bplus:
	@echo "Compile bplus_main ..."
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bp_main.c ./src/record.c ./src/bp_file.c ./src/bp_datanode.c ./src/bp_indexnode.c -lbf -o ./build/bplus_main -O2

clean:
	@echo "Cleaning up build files ..."
	rm -rf ./build/bplus_main
	rm -rf ./data.db

run10: build
	@echo "Running bplus_main ..."
	./build/bplus_main 10

run100: build
	@echo "Running bplus_main ..."
	./build/bplus_main 100

run1000: build
	@echo "Running bplus_main ..."
	./build/bplus_main 1000

run10000: build
	@echo "Running bplus_main ..."
	./build/bplus_main 10000