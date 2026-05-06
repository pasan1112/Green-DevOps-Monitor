pipeline {
    agent any

    environment {
        APP_IMAGE = "green-devops-sample-app"
        APP_CONTAINER = "green-devops-sample-app"
        APP_PORT = "5052"
        RUN_ID = "${env.BUILD_TAG}"
        ELECTRICITYMAPS_API_KEY = BpjVnaE4hWm9CE947xSP
    }

    stages {
        stage('Build') {
            steps {
                sh '''
                docker run --rm \
                -e ELECTRICITYMAPS_API_KEY=$ELECTRICITYMAPS_API_KEY \
                -v "$WORKSPACE":/app \
                -w /app \
                python:3.12-slim \
                bash -c "pip install -r requirements.txt && python monitor_runner.py --stage build --run-id $RUN_ID --cmd 'python -m pip install -r sample_app/requirements.txt'"
                '''
            }
        }

        stage('Test') {
            steps {
                sh '''
                docker run --rm \
                -e ELECTRICITYMAPS_API_KEY=$ELECTRICITYMAPS_API_KEY \
                -v "$WORKSPACE":/app \
                -w /app \
                python:3.12-slim \
                bash -c "pip install -r requirements.txt && python monitor_runner.py --stage test --run-id $RUN_ID --cmd 'pytest sample_app/tests'"
                '''
            }
        }

        stage('Deploy') {
            steps {
                sh '''
                python3 -m pip install psutil requests pandas || true

                python3 monitor_runner.py \
                --stage deploy \
                --run-id "$RUN_ID" \
                --cmd "docker rm -f $APP_CONTAINER || true && docker build -t $APP_IMAGE ./sample_app && docker run -d --name $APP_CONTAINER -p $APP_PORT:5052 $APP_IMAGE"
                '''
            }
        }
    }
}