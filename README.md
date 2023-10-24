# ipamConsistencyMonitoring
database 정합성을 검사하고 정합성이 무너질 경우 로그를 만듭니다.


* background 실행 cmd

nohup python3 dbSelect.py > /dev/null 2>&1 &
