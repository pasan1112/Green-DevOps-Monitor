pipeline {
    agent any

    environment {
        APP_IMAGE = "green-devops-sample-app"
        APP_CONTAINER = "green-devops-sample-app"
        APP_PORT = "5052"
        RUN_ID = "${env.BUILD_TAG}"
        ELECTRICITYMAPS_API_KEY = "BpjVnaE4hWm9CE947xSP"
    }

    stages {

        stage('Build') {
            steps {
                sh '''
                docker run --rm \
                --volumes-from jenkins \
                -e ELECTRICITYMAPS_API_KEY=$ELECTRICITYMAPS_API_KEY \
                -w "$WORKSPACE" \
                python:3.12-slim \
                sh -c "pip install -r requirements.txt && python monitor_runner.py --stage build --run-id $RUN_ID --cmd 'python -m pip install -r sample_app/requirements.txt'"
                '''
            }
        }

        stage('Test') {
            steps {
                sh '''
                docker run --rm \
                --volumes-from jenkins \
                -e ELECTRICITYMAPS_API_KEY=$ELECTRICITYMAPS_API_KEY \
                -w "$WORKSPACE" \
                python:3.12-slim \
                sh -c "pip install -r requirements.txt && python monitor_runner.py --stage test --run-id $RUN_ID --cmd 'pytest sample_app/tests'"
                '''
            }
        }

        stage('Deploy') {
            steps {
                sh '''
                docker rm -f $APP_CONTAINER || true
                docker build -t $APP_IMAGE ./sample_app

                docker run --rm \
                --volumes-from jenkins \
                -v /var/run/docker.sock:/var/run/docker.sock \
                -v /usr/bin/docker:/usr/bin/docker \
                -e ELECTRICITYMAPS_API_KEY=$ELECTRICITYMAPS_API_KEY \
                -w "$WORKSPACE" \
                python:3.12-slim \
                sh -c "pip install -r requirements.txt && python monitor_runner.py --stage deploy --run-id $RUN_ID --cmd 'docker run -d --name $APP_CONTAINER -p $APP_PORT:5052 $APP_IMAGE'"
                '''
            }
        }
    }
}