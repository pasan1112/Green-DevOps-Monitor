pipeline {
    agent any

    environment {
        APP_IMAGE = "green-real-backend"
        APP_CONTAINER = "green-real-backend"
        APP_PORT = "5053"
        RUN_ID = "${env.BUILD_TAG}"

        ELECTRICITYMAPS_API_KEY = "BpjVnaE4hWm9CE947xSP"
        MONGO_URI = 'mongodb+srv://admin:admin1234@green-devops-monitor.xxflzzs.mongodb.net/?retryWrites=true&w=majority&appName=Green-DevOps-Monitor'
    }

    stages {

        stage('Build') {
            steps {
                sh '''
                STAGE_START=$(date +%s.%N)
                STAGE_START_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
                MONITOR_OUTPUT=$(mktemp)

                set +e
                docker run --rm \
                --volumes-from jenkins \
                -e ELECTRICITYMAPS_API_KEY="$ELECTRICITYMAPS_API_KEY" \
                -e MONGO_URI="$MONGO_URI" \
                -w "$WORKSPACE" \
                nikolaik/python-nodejs:python3.12-nodejs20 \
                sh -c "python monitor_runner.py --stage build --run-id $RUN_ID --cmd 'pip install -r requirements.txt && cd real_backend && npm install && npm run build'" \
                > "$MONITOR_OUTPUT" 2>&1
                STAGE_RC=$?
                set -e
                cat "$MONITOR_OUTPUT"

                STAGE_END=$(date +%s.%N)
                STAGE_END_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
                STAGE_DURATION=$(python3 -c "print(round($STAGE_END - $STAGE_START, 4))")
                WORKLOAD_DURATION=$(grep 'WORKLOAD_DURATION_SECONDS=' "$MONITOR_OUTPUT" | tail -n 1 | cut -d= -f2)
                if [ -z "$WORKLOAD_DURATION" ]; then
                    WORKLOAD_DURATION="$STAGE_DURATION"
                fi

                docker run --rm \
                --volumes-from jenkins \
                -e MONGO_URI="$MONGO_URI" \
                -w "$WORKSPACE" \
                nikolaik/python-nodejs:python3.12-nodejs20 \
                sh -c "python monitor_runner.py --stage build --run-id $RUN_ID --workload-duration $WORKLOAD_DURATION --jenkins-stage-duration $STAGE_DURATION --stage-start-timestamp '$STAGE_START_TS' --stage-end-timestamp '$STAGE_END_TS'" || true

                rm -f "$MONITOR_OUTPUT"
                exit $STAGE_RC
                '''
            }
        }

        stage('Test') {
            steps {
                sh '''
                STAGE_START=$(date +%s.%N)
                STAGE_START_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
                MONITOR_OUTPUT=$(mktemp)

                set +e
                docker run --rm \
                --volumes-from jenkins \
                -e ELECTRICITYMAPS_API_KEY="$ELECTRICITYMAPS_API_KEY" \
                -e MONGO_URI="$MONGO_URI" \
                -w "$WORKSPACE" \
                nikolaik/python-nodejs:python3.12-nodejs20 \
                sh -c "python monitor_runner.py --stage test --run-id $RUN_ID --cmd 'pip install -r requirements.txt && cd real_backend && npm test'" \
                > "$MONITOR_OUTPUT" 2>&1
                STAGE_RC=$?
                set -e
                cat "$MONITOR_OUTPUT"

                STAGE_END=$(date +%s.%N)
                STAGE_END_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
                STAGE_DURATION=$(python3 -c "print(round($STAGE_END - $STAGE_START, 4))")
                WORKLOAD_DURATION=$(grep 'WORKLOAD_DURATION_SECONDS=' "$MONITOR_OUTPUT" | tail -n 1 | cut -d= -f2)
                if [ -z "$WORKLOAD_DURATION" ]; then
                    WORKLOAD_DURATION="$STAGE_DURATION"
                fi

                docker run --rm \
                --volumes-from jenkins \
                -e MONGO_URI="$MONGO_URI" \
                -w "$WORKSPACE" \
                nikolaik/python-nodejs:python3.12-nodejs20 \
                sh -c "python monitor_runner.py --stage test --run-id $RUN_ID --workload-duration $WORKLOAD_DURATION --jenkins-stage-duration $STAGE_DURATION --stage-start-timestamp '$STAGE_START_TS' --stage-end-timestamp '$STAGE_END_TS'" || true

                rm -f "$MONITOR_OUTPUT"
                exit $STAGE_RC
                '''
            }
        }

        stage('Deploy') {
            steps {
                sh '''
                STAGE_START=$(date +%s.%N)
                STAGE_START_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
                MONITOR_OUTPUT=$(mktemp)

                set +e
                docker run --rm \
                --volumes-from jenkins \
                -v /var/run/docker.sock:/var/run/docker.sock \
                -v /usr/bin/docker:/usr/bin/docker \
                -e ELECTRICITYMAPS_API_KEY="$ELECTRICITYMAPS_API_KEY" \
                -e MONGO_URI="$MONGO_URI" \
                -e APP_IMAGE="$APP_IMAGE" \
                -e APP_CONTAINER="$APP_CONTAINER" \
                -e APP_PORT="$APP_PORT" \
                -w "$WORKSPACE" \
                nikolaik/python-nodejs:python3.12-nodejs20 \
                sh -c "python monitor_runner.py --stage deploy --run-id $RUN_ID --cmd 'pip install -r requirements.txt && docker build -t $APP_IMAGE ./real_backend && (docker rm -f $APP_CONTAINER || true) && docker run -d --name $APP_CONTAINER -p $APP_PORT:3000 $APP_IMAGE'" \
                > "$MONITOR_OUTPUT" 2>&1
                STAGE_RC=$?
                set -e
                cat "$MONITOR_OUTPUT"

                STAGE_END=$(date +%s.%N)
                STAGE_END_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
                STAGE_DURATION=$(python3 -c "print(round($STAGE_END - $STAGE_START, 4))")
                WORKLOAD_DURATION=$(grep 'WORKLOAD_DURATION_SECONDS=' "$MONITOR_OUTPUT" | tail -n 1 | cut -d= -f2)
                if [ -z "$WORKLOAD_DURATION" ]; then
                    WORKLOAD_DURATION="$STAGE_DURATION"
                fi

                docker run --rm \
                --volumes-from jenkins \
                -e MONGO_URI="$MONGO_URI" \
                -w "$WORKSPACE" \
                nikolaik/python-nodejs:python3.12-nodejs20 \
                sh -c "python monitor_runner.py --stage deploy --run-id $RUN_ID --workload-duration $WORKLOAD_DURATION --jenkins-stage-duration $STAGE_DURATION --stage-start-timestamp '$STAGE_START_TS' --stage-end-timestamp '$STAGE_END_TS'" || true

                rm -f "$MONITOR_OUTPUT"
                exit $STAGE_RC
                '''
            }
        }
    }
}
