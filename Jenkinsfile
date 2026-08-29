@Library('green-agent') _

// ─────────────────────────────────────────────────────────────────────────
// Groovy helpers
// ─────────────────────────────────────────────────────────────────────────
def greenReleaseModules() {
    return ['core', 'service', 'api', 'app']
}

def buildMockCarbonData() {
    def now = new Date()
    def entries = (0..24).collect { h ->
        def ts = new Date(now.time - (24 - h) * 3600000L)
        def isoTs = ts.format("yyyy-MM-dd'T'HH:mm:ss")
        def intensity = String.format("%.1f", 310.0 + h * 0.4)
        "{\"timestamp\":\"${isoTs}\",\"intensity\":${intensity}}"
    }
    return [
        history:  '[' + entries.join(',') + ']',
        forecast: '[{"hour":1,"intensity":300.0},{"hour":2,"intensity":250.0},{"hour":3,"intensity":180.0}]'
    ]
}

def serializeCarbonHistory(histList) {
    def entries = histList.collect { h ->
        "{\"timestamp\":\"${h.timestamp}\",\"intensity\":${h.intensity}}"
    }
    return '[' + entries.join(',') + ']'
}

def serializeCarbonForecast(forecastList) {
    def entries = forecastList.collect { f ->
        "{\"hour\":${f.hour},\"intensity\":${f.intensity}}"
    }
    return '[' + entries.join(',') + ']'
}

def discoverModuleTestInventory() {
    def inventory = [:]
    greenReleaseModules().each { moduleName ->
        def countText = sh(
            script: '''
                module="''' + moduleName + '''"
                if [ -d "$module/src/test/java" ]; then
                  grep -Rho '@Test' "$module/src/test/java" --include='*.java' 2>/dev/null | wc -l | tr -d ' '
                else
                  echo 0
                fi
            ''',
            returnStdout: true
        ).trim()
        inventory[moduleName] = countText ? countText.toInteger() : 0
    }
    return inventory
}

def readSurefireTestCounts() {
    def counts = [:]
    greenReleaseModules().each { moduleName ->
        def countText = sh(
            script: '''
                module="''' + moduleName + '''"
                report_dir="$module/target/surefire-reports"
                if [ -d "$report_dir" ] && find "$report_dir" -name 'TEST-*.xml' -type f | grep -q .; then
                  find "$report_dir" -name 'TEST-*.xml' -type f -exec awk 'BEGIN { total = 0 } /<testsuite / { if (match($0, /tests="[0-9]+"/)) { value = substr($0, RSTART + 7, RLENGTH - 8); total += value } } END { print total }' {} +
                else
                  echo 0
                fi
            ''',
            returnStdout: true
        ).trim()
        counts[moduleName] = countText ? countText.toInteger() : 0
    }
    return counts
}

def affectedModuleSet() {
    if (env.AFFECTED_MODULES == 'all') {
        return greenReleaseModules() as Set
    }
    if (!env.AFFECTED_MODULES?.trim()) {
        return [] as Set
    }
    return env.AFFECTED_MODULES.split(',').collect { it.trim() }.findAll { it } as Set
}

def appAffected() {
    return env.AFFECTED_MODULES == 'all' || env.AFFECTED_MODULES?.split(',')?.contains('app')
}

def monitorRunId() {
    return "${env.JOB_NAME}-${env.BUILD_NUMBER}"
}

def monitorPipelineStatus() {
    def result = currentBuild.currentResult ?: 'SUCCESS'
    return result == 'SUCCESS' ? 'success' : 'failed'
}

def monitorReturnCode() {
    def result = currentBuild.currentResult ?: 'SUCCESS'
    return result == 'SUCCESS' ? '0' : '1'
}

def releaseWorkPlanned() {
    def hasBuildCommands = env.MAVEN_BUILD_COMMANDS?.trim() ? true : false
    def hasTestCommands = env.MAVEN_TEST_COMMANDS?.trim() ? true : false
    def hasDockerBuild = appAffected()
    return !params.DRY_RUN &&
        env.OPTIMIZER_STATUS == 'success' &&
        (hasBuildCommands || hasTestCommands || hasDockerBuild)
}

def deployWorkPlanned() {
    return !params.DRY_RUN && appAffected()
}

def releaseSkipReason() {
    if (params.DRY_RUN) {
        return 'dry_run'
    }
    if (!env.AFFECTED_MODULES?.trim()) {
        return 'no_affected_components'
    }
    return 'no_release_work'
}

def deploySkipReason() {
    if (params.DRY_RUN) {
        return 'dry_run'
    }
    if (!appAffected()) {
        return 'app_not_affected'
    }
    return 'deployment_not_required'
}

def monitorCommand(String lifecycleStage, String action, String skipReason = '') {
    try {
        def statusArgs = ''
        if (action == 'stop') {
            statusArgs = " --status \"${monitorPipelineStatus()}\" --return-code \"${monitorReturnCode()}\""
        }
        def reasonArgs = ''
        if (action == 'skip') {
            reasonArgs = " --reason \"${skipReason}\""
        }
        def rc = timeout(time: 1, unit: 'MINUTES') {
            sh(
                script: """
                    cd "${env.MONITOR_HOME}"
                    "${env.MONITOR_PYTHON}" monitor_runner.py ${action} --stage ${lifecycleStage} --pipeline "${env.JOB_NAME}" --run-id "${monitorRunId()}" --zone "${env.MONITOR_ZONE}"${statusArgs}${reasonArgs}
                """,
                returnStatus: true
            )
        }
        if (rc != 0) {
            echo "[Monitor] WARNING: ${action.toUpperCase()} ${lifecycleStage} returned ${rc}. Continuing pipeline."
        }
        return rc
    } catch (Exception monitorError) {
        echo "[Monitor] WARNING: ${action.toUpperCase()} ${lifecycleStage} failed: ${monitorError}. Continuing pipeline."
        return 1
    }
}

def startMonitorSession(String lifecycleStage) {
    if (lifecycleStage == 'release') {
        if (env.RELEASE_MONITOR_STARTED == 'true' || env.RELEASE_MONITOR_FINISHED == 'true' || env.RELEASE_MONITOR_SKIPPED == 'true') {
            echo "[Monitor] release monitor already started, finished, or skipped for this run."
            return
        }
        if (monitorCommand('release', 'start') == 0) {
            env.RELEASE_MONITOR_STARTED = 'true'
        }
        return
    }

    if (lifecycleStage == 'deploy') {
        if (env.DEPLOY_MONITOR_STARTED == 'true' || env.DEPLOY_MONITOR_FINISHED == 'true' || env.DEPLOY_MONITOR_SKIPPED == 'true') {
            echo "[Monitor] deploy monitor already started, finished, or skipped for this run."
            return
        }
        if (monitorCommand('deploy', 'start') == 0) {
            env.DEPLOY_MONITOR_STARTED = 'true'
        }
        return
    }

    echo "[Monitor] WARNING: Unsupported monitor lifecycle '${lifecycleStage}'. Continuing pipeline."
}

def finishMonitorSession(String lifecycleStage, String action = 'stop') {
    if (lifecycleStage == 'release') {
        if (env.RELEASE_MONITOR_STARTED != 'true' || env.RELEASE_MONITOR_FINISHED == 'true') {
            return
        }
        monitorCommand('release', action)
        env.RELEASE_MONITOR_FINISHED = 'true'
        return
    }

    if (lifecycleStage == 'deploy') {
        if (env.DEPLOY_MONITOR_STARTED != 'true' || env.DEPLOY_MONITOR_FINISHED == 'true') {
            return
        }
        monitorCommand('deploy', action)
        env.DEPLOY_MONITOR_FINISHED = 'true'
        return
    }

    echo "[Monitor] WARNING: Unsupported monitor lifecycle '${lifecycleStage}'. Continuing pipeline."
}

def skipMonitorSession(String lifecycleStage, String reason) {
    if (lifecycleStage == 'release') {
        if (env.RELEASE_MONITOR_STARTED == 'true' || env.RELEASE_MONITOR_FINISHED == 'true' || env.RELEASE_MONITOR_SKIPPED == 'true') {
            echo "[Monitor] release monitor already has an outcome for this run."
            return
        }
        monitorCommand('release', 'skip', reason)
        env.RELEASE_MONITOR_SKIPPED = 'true'
        return
    }

    if (lifecycleStage == 'deploy') {
        if (env.DEPLOY_MONITOR_STARTED == 'true' || env.DEPLOY_MONITOR_FINISHED == 'true' || env.DEPLOY_MONITOR_SKIPPED == 'true') {
            echo "[Monitor] deploy monitor already has an outcome for this run."
            return
        }
        monitorCommand('deploy', 'skip', reason)
        env.DEPLOY_MONITOR_SKIPPED = 'true'
        return
    }

    echo "[Monitor] WARNING: Unsupported monitor lifecycle '${lifecycleStage}'. Continuing pipeline."
}

pipeline {
    agent any

    parameters {
        booleanParam(
            name: 'DRY_RUN',
            defaultValue: false,
            description: 'When true, only run the optimizer analysis without building, testing, or deploying.'
        )
        booleanParam(
            name: 'FORCE_FULL_BUILD',
            defaultValue: false,
            description: 'Skip the optimizer analysis and force a full build and test of all modules.'
        )
        booleanParam(
            name: 'ENABLE_GREEN_SCHEDULING',
            defaultValue: true,
            description: 'Allow the pipeline to delay the build until a greener time window.'
        )
        string(
            name: 'OVERRIDE_SCHEDULE_HOUR',
            defaultValue: 'auto',
            description: 'Override ML recommendation. Use "auto" to let the ML model decide.'
        )
        string(
            name: 'PRE_SELECTED_STRATEGY',
            defaultValue: '',
            description: 'Internal: pre-selected deploy strategy carried over from a previously rescheduled build. Set automatically — do not change manually.'
        )
    }

    options {
        disableConcurrentBuilds()
    }

    environment {
        DOCKER_IMAGE             = 'hiranx/green-release-app'
        DOCKER_TAG               = "${BUILD_NUMBER}"
        DOCKER_HUB_CREDENTIALS   = credentials('dockerhub-hiran-credentials')
        DASHBOARD_URL            = 'http://host.docker.internal:5005'
        ELECTRICITY_MAPS_API_KEY = 'em_nGgVAPUefFX2qe8BkqzFgw3n8uGpJE2J'

        REMOTE_HOST         = '147.15.144.192'
        REMOTE_PORT         = '2510'
        REMOTE_USER         = 'hiran'
        SSH_CREDENTIALS     = 'ubuntu-pc-ssh-hiran'

        METRICS_URL         = 'http://172.17.0.1:5001'
        GREEN_AGENT_URL     = 'http://172.17.0.1:5002'

        CANARY_WEIGHT       = '20'
        CANARY_WAIT_SECS    = '60'
        CANARY_CONTAINER    = 'green-release-canary'
        ROLLING_WAIT_SECS   = '15'
        MONITOR_HOME        = '/home/pasan/green-devops-monitor-dashboard'
        MONITOR_PYTHON      = '/home/pasan/green-devops-monitor-dashboard/jenkins-venv/bin/python'
        MONITOR_ZONE        = 'LK'

        // ── FIX: DEPLOY_STRATEGY removed from here. ─────────────────────
        // WHY: Variables declared in the top-level `environment {}` block
        // get a special, effectively locked binding in declarative
        // pipelines. Later reassigning it with `env.DEPLOY_STRATEGY = ...`
        // inside a script/shared-library step does NOT reliably override
        // reads of `${env.DEPLOY_STRATEGY}` elsewhere in the pipeline -
        // every `when { expression { env.DEPLOY_STRATEGY == '...' } }`
        // block kept seeing the literal 'rolling' declared here, no
        // matter what greenCheck() actually selected (canary/recreate).
        // The default is now set dynamically in 'Init Build Metadata'
        // instead, where it behaves like a normal mutable env var.
    }

    stages {

        stage('Init Build Metadata') {
            steps {
                script {
                    env.PIPELINE_START    = System.currentTimeMillis().toString()
                    env.COMMIT_SHA        = ''
                    env.COMMIT_MSG        = ''
                    env.WORK_DIR          = fileExists('pom.xml') ? '.' : 'green-release-demo'
                    env.DOCKER_PUSH_OK    = 'false'
                    env.DEPLOY_STRATEGY   = 'rolling'
                    env.RELEASE_MONITOR_STARTED  = 'false'
                    env.RELEASE_MONITOR_FINISHED = 'false'
                    env.RELEASE_MONITOR_SKIPPED  = 'false'
                    env.DEPLOY_MONITOR_STARTED   = 'false'
                    env.DEPLOY_MONITOR_FINISHED  = 'false'
                    env.DEPLOY_MONITOR_SKIPPED   = 'false'
                    // Carry forward pre-selected strategy from a rescheduled build
                    if (params.PRE_SELECTED_STRATEGY?.trim()) {
                        env.DEPLOY_STRATEGY = params.PRE_SELECTED_STRATEGY.toString().toLowerCase().trim()
                        echo "🌿 Carrying forward pre-selected deploy strategy: ${env.DEPLOY_STRATEGY}"
                    }
                }
            }
        }

        stage('Setup Tools') {
            steps {
                script {
                    def workspace = pwd()
                    echo "Setting up local tool binaries (Docker CLI, Maven, and Docker Compose)..."
                    sh 'mkdir -p tool-bin'

                    if (sh(script: 'command -v docker >/dev/null 2>&1', returnStatus: true) != 0) {
                        if (!fileExists('tool-bin/docker')) {
                            echo "Docker CLI not found. Downloading static binary..."
                            sh '''
                                curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-27.3.1.tgz -o docker.tgz
                                tar -xzf docker.tgz --strip-components=1 -C tool-bin docker/docker
                                rm -f docker.tgz
                                chmod +x tool-bin/docker
                            '''
                        }
                    } else {
                        echo "System docker command is already available."
                    }

                    if (sh(script: 'command -v docker-compose >/dev/null 2>&1', returnStatus: true) != 0) {
                        if (!fileExists('tool-bin/docker-compose')) {
                            echo "Docker Compose not found. Downloading static binary..."
                            sh '''
                                curl -fsSL https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-x86_64 -o tool-bin/docker-compose
                                chmod +x tool-bin/docker-compose
                            '''
                        }
                    } else {
                        echo "System docker-compose command is already available."
                    }

                    if (sh(script: 'command -v mvn >/dev/null 2>&1', returnStatus: true) != 0) {
                        if (!fileExists('tool-bin/maven/bin/mvn')) {
                            echo "Maven not found. Downloading Apache Maven..."
                            sh '''
                                curl -fsSL https://archive.apache.org/dist/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.tar.gz -o maven.tar.gz
                                mkdir -p tool-bin/maven
                                tar -xzf maven.tar.gz -C tool-bin/maven --strip-components=1
                                rm -f maven.tar.gz
                                chmod +x tool-bin/maven/bin/mvn
                            '''
                        }
                    } else {
                        echo "System mvn command is already available."
                    }

                    env.PATH = "${workspace}/tool-bin:${workspace}/tool-bin/maven/bin:${env.PATH}"
                    sh 'docker --version'
                    sh 'mvn --version'
                }
            }
        }

        stage('Checkout') {
            steps {
                script {
                    sh "git config --global --add safe.directory '*'"
                    if (!fileExists('pom.xml')) {
                        echo "Checking out green-release-demo..."
                        dir(env.WORK_DIR) {
                            checkout([$class: 'GitSCM',
                                branches: [[name: '*/main']],
                                userRemoteConfigs: [[url: 'https://github.com/Beliver-247/green-release-demo.git']],
                                extensions: [[$class: 'CloneOption', shallow: false, depth: 0, noTags: false]]
                            ])
                        }
                    } else {
                        echo "Found pom.xml in workspace root. Using existing root checkout."
                        sh "git status || echo 'Not a git repo'"
                        sh "git rev-parse HEAD || echo 'No git commit'"
                        echo "Forcing checkout of latest main..."
                        sh """
                            git fetch origin main || true
                            git checkout -B main origin/main || true
                            git pull origin main || true
                        """
                    }
                    dir(env.WORK_DIR) {
                        env.COMMIT_SHA = sh(script: 'git rev-parse HEAD', returnStdout: true).trim()
                        env.COMMIT_MSG = sh(script: 'git log -1 --pretty=%s', returnStdout: true).trim()
                        env.URGENT_DEPLOY = env.COMMIT_MSG.toLowerCase().contains('[urgent]') ? 'true' : 'false'
                        env.BYPASS_GREEN = env.COMMIT_MSG.toLowerCase().contains('[bypass-green]') ? 'true' : 'false'
                        if (env.URGENT_DEPLOY == 'true' || env.BYPASS_GREEN == 'true') {
                            echo "⚡ URGENT/BYPASS deployment detected — all green checks will be skipped"
                        }
                        env.GIT_PREVIOUS_SUCCESSFUL_COMMIT = fileExists('.last_built_commit') ? readFile('.last_built_commit').trim() : 'null'
                        // Explicitly set GIT_COMMIT and GIT_PREVIOUS_COMMIT so the optimizer
                        // receives the correct diff range. Jenkins does NOT auto-populate these
                        // for manual/scripted checkouts — they stay null, causing the optimizer
                        // to be unable to resolve the diff base inside the Docker container.
                        env.GIT_COMMIT         = env.COMMIT_SHA
                        env.GIT_PREVIOUS_COMMIT = sh(script: 'git rev-parse HEAD~1 2>/dev/null || echo null', returnStdout: true).trim()
                    }
                }
            }
        }

        stage('Build Optimizer - Analyze') {
            steps {
                dir(env.WORK_DIR) {
                    script {
                        if (params.FORCE_FULL_BUILD) {
                            echo "=== FORCE FULL BUILD ==="
                            echo "Skipping optimizer module analysis. Green AI strategy selection will run after the build."

                            // Force ALL modules to be built and tested
                            env.OPTIMIZER_STATUS     = 'success'
                            env.AFFECTED_MODULES     = 'all'
                            env.MAVEN_BUILD_COMMANDS = 'mvn clean install -DskipTests'
                            env.MAVEN_TEST_COMMANDS  = 'mvn test'
                            env.OPTIMIZER_DURATION   = '0'

                            return
                        }

                        def analyzeStart = System.currentTimeMillis()
                        def output = sh(
                            script: '''
                                EXIT_CODE=0
                                tar -C "$PWD" -cf - . | docker run --rm -i \
                                  -e ELECTRICITY_MAPS_API_KEY="''' + (env.ELECTRICITY_MAPS_API_KEY ?: '') + '''" \
                                  -e GIT_PREVIOUS_SUCCESSFUL_COMMIT="''' + env.GIT_PREVIOUS_SUCCESSFUL_COMMIT + '''" \
                                  -e GIT_PREVIOUS_COMMIT="''' + env.GIT_PREVIOUS_COMMIT + '''" \
                                  -e GIT_COMMIT="''' + env.GIT_COMMIT + '''" \
                                  -v /var/run/docker.sock:/var/run/docker.sock \
                                  --entrypoint "" \
                                  beliver247/build-optimizer-agent:latest \
                                  bash -lc '
                                    set -e
                                    mkdir -p /work
                                    tar -xf - -C /work
                                    cd /work
                                    git config --global --add safe.directory /work
                                    python3 -m optimizer \
                                      --project-root /work \
                                      --dry-run true \
                                      --output-format json \
                                      --carbon-aware
                                  ' || EXIT_CODE=$?
                                if [ "$EXIT_CODE" -eq 1 ]; then
                                  echo "OPTIMIZER_ERROR"
                                  exit 1
                                fi
                            ''',
                            returnStdout: true
                        ).trim()

                        env.OPTIMIZER_DURATION = ((System.currentTimeMillis() - analyzeStart) / 1000.0).toString()

                        echo "=== Build Optimizer Output ==="
                        echo output
                        echo "=============================="

                        def jsonLine = output.readLines().find { it.startsWith('{"') }
                        if (jsonLine) {
                            def result = new groovy.json.JsonSlurper().parseText(jsonLine)
                            env.OPTIMIZER_STATUS = result.status ?: 'unknown'
                            def buildCommands = []
                            def testCommands = []
                            for (action in result.actions) {
                                if (action.name == 'build') buildCommands.add(action.command.join(' '))
                                else if (action.name == 'test') testCommands.add(action.command.join(' '))
                            }
                            env.MAVEN_BUILD_COMMANDS = buildCommands.join('|||')
                            env.MAVEN_TEST_COMMANDS  = testCommands.join('|||')
                            env.AFFECTED_MODULES     = (result.affected_modules ?: []).join(',')

                            echo "Optimizer status: ${env.OPTIMIZER_STATUS}"
                            echo "Affected modules: ${env.AFFECTED_MODULES}"
                            echo "Build commands: ${env.MAVEN_BUILD_COMMANDS}"
                            echo "Test commands: ${env.MAVEN_TEST_COMMANDS}"

                            if (result.scheduling) {
                                env.CARBON_INTENSITY    = result.scheduling.current_intensity?.toString() ?: ''
                                env.GREEN_PROBABILITY   = result.scheduling.green_probability?.toString() ?: ''
                                env.SCHEDULING_ACTION   = result.scheduling.action ?: ''
                                env.SCHEDULING_ENGINE   = result.scheduling.engine ?: ''
                                env.SCHEDULED_HOUR      = result.scheduling.scheduled_hour?.toString() ?: ''
                                env.TARGET_INTENSITY    = result.scheduling.target_intensity?.toString() ?: ''
                                if (result.scheduling.carbon_history) {
                                    env.CARBON_HISTORY = serializeCarbonHistory(result.scheduling.carbon_history)
                                } else {
                                    echo "[GreenOptimizer] carbon_history missing from optimizer output. Using mock history."
                                    env.CARBON_HISTORY = buildMockCarbonData().history
                                }
                                if (result.scheduling.carbon_forecast) {
                                    env.CARBON_FORECAST = serializeCarbonForecast(result.scheduling.carbon_forecast)
                                } else {
                                    env.CARBON_FORECAST = buildMockCarbonData().forecast
                                }
                            } else {
                                echo "[GreenOptimizer] No scheduling data in optimizer output. Using mock carbon data."
                                def mockCarbon = buildMockCarbonData()
                                env.CARBON_INTENSITY  = '320.0'
                                env.GREEN_PROBABILITY = '0.35'
                                env.SCHEDULING_ACTION = 'execute_now'
                                env.SCHEDULING_ENGINE = 'mock'
                                env.SCHEDULED_HOUR    = ''
                                env.TARGET_INTENSITY  = ''
                                env.CARBON_HISTORY    = mockCarbon.history
                                env.CARBON_FORECAST   = mockCarbon.forecast
                            }
                        } else {
                            env.OPTIMIZER_STATUS     = 'no_changes'
                            env.MAVEN_BUILD_COMMANDS = ''
                            env.MAVEN_TEST_COMMANDS  = ''
                            env.AFFECTED_MODULES     = ''
                        }

                        if (params.DRY_RUN) {
                            echo "=== DRY RUN MODE — Skipping build, test, Docker, and deploy stages ==="
                        }
                    }
                }
            }
        }

        stage('Green Scheduling') {
            when {
                expression {
                    params.ENABLE_GREEN_SCHEDULING &&
                    env.OPTIMIZER_STATUS == 'success' &&
                    env.URGENT_DEPLOY != 'true' &&
                    env.BYPASS_GREEN != 'true'
                }
            }
            steps {
                script {
                    // ────────────────────────────────────────────────────────────────────
                    // PRE-FLIGHT PHASE
                    //
                    // Query BOTH schedulers here, before any build work,
                    // to agree on ONE green window and ONE deploy strategy.
                    //
                    // greenSchedule() internally:
                    //   1. Reads ML Optimizer output already in env vars
                    //   2. Makes a single-shot call to the Green AI Agent
                    //   3. Combines signals into one SchedulingDecision
                    // ────────────────────────────────────────────────────────────────────
                    def decision = greenSchedule(
                        schedulingAction : env.SCHEDULING_ACTION,
                        scheduledHour    : env.SCHEDULED_HOUR,
                        greenProbability : env.GREEN_PROBABILITY,
                        carbonIntensity  : env.CARBON_INTENSITY,
                        overrideHour     : params.OVERRIDE_SCHEDULE_HOUR,
                        urgentDeploy     : false,  // already gated in when{} above
                        agentUrl         : env.GREEN_AGENT_URL
                    )

                    // Store all decision fields for dashboard and downstream stages
                    env.COMBINED_CONFIDENCE   = decision.mlGreenProbability.toString()
                    env.ML_GREEN_PROBABILITY  = decision.mlGreenProbability.toString()
                    env.AI_CONFIDENCE         = decision.aiConfidence.toString()
                    env.BOTH_SCHEDULERS_AGREE = "N/A"
                    env.SCHEDULING_REASON     = decision.reason.toString()

                    if (decision.shouldSchedule) {
                        // Pre-select deploy strategy NOW so it survives the reschedule
                        env.DEPLOY_STRATEGY = decision.preSelectedStrategy

                        build job: env.JOB_NAME,
                              quietPeriod: decision.delaySeconds,
                              wait: false,
                              parameters: [
                                  booleanParam(name: 'ENABLE_GREEN_SCHEDULING', value: false),
                                  booleanParam(name: 'DRY_RUN',                 value: params.DRY_RUN),
                                  booleanParam(name: 'FORCE_FULL_BUILD',        value: params.FORCE_FULL_BUILD),
                                  string(name: 'OVERRIDE_SCHEDULE_HOUR',        value: 'auto'),
                                  string(name: 'PRE_SELECTED_STRATEGY',         value: decision.preSelectedStrategy)
                              ]

                        currentBuild.description = "🌿 Rescheduled for ${decision.scheduledHour}:00 | Strategy: ${decision.preSelectedStrategy} | ML Prob: ${String.format('%.2f', decision.mlGreenProbability)}"
                        env.IS_RESCHEDULED = 'true'
                        currentBuild.result = 'ABORTED'
                        error("Pipeline rescheduled to a greener window at ${decision.scheduledHour}:00. Strategy pre-selected: ${decision.preSelectedStrategy}.")
                    }

                    // Green now — set strategy and continue straight to build
                    env.DEPLOY_STRATEGY = decision.preSelectedStrategy
                    echo "🌿 Green window confirmed right now. Strategy pre-selected: ${env.DEPLOY_STRATEGY}"
                    echo "📊 ML Probability: ${String.format('%.2f', decision.mlGreenProbability)} | AI Strategy Confidence: ${String.format('%.2f', decision.aiConfidence)}"
                }
            }
        }

        stage('Skip Release Monitor') {
            when {
                expression { !releaseWorkPlanned() }
            }
            steps {
                script {
                    skipMonitorSession('release', releaseSkipReason())
                }
            }
        }

        stage('Start Release Monitor') {
            when {
                expression { releaseWorkPlanned() }
            }
            steps {
                script {
                    startMonitorSession('release')
                }
            }
        }

        stage('Selective Build') {
            when {
                expression { !params.DRY_RUN && env.OPTIMIZER_STATUS == 'success' && env.MAVEN_BUILD_COMMANDS?.trim() }
            }
            steps {
                dir(env.WORK_DIR) {
                    script {
                        def buildStart = System.currentTimeMillis()
                        echo "Running selective Maven build for modules: ${env.AFFECTED_MODULES}"
                        env.MAVEN_BUILD_COMMANDS.split('\\|\\|\\|').each { cmd ->
                            echo "Executing: ${cmd}"
                            sh cmd
                        }
                        env.BUILD_DURATION = ((System.currentTimeMillis() - buildStart) / 1000.0).toString()
                    }
                }
            }
        }

        stage('Selective Test') {
            when {
                expression { !params.DRY_RUN && env.OPTIMIZER_STATUS == 'success' && env.MAVEN_TEST_COMMANDS?.trim() }
            }
            steps {
                dir(env.WORK_DIR) {
                    script {
                        def testStart = System.currentTimeMillis()
                        echo "Running selective tests for modules: ${env.AFFECTED_MODULES}"
                        def testOutput = ''
                        env.MAVEN_TEST_COMMANDS.split('\\|\\|\\|').each { cmd ->
                            echo "Executing: ${cmd}"
                            testOutput += sh(script: cmd, returnStdout: true)
                        }
                        env.TEST_DURATION = ((System.currentTimeMillis() - testStart) / 1000.0).toString()

                        def testsRun = 0
                        def testsSkipped = 0
                        def moduleDetails = [:]
                        def inventory = discoverModuleTestInventory()
                        def executed  = readSurefireTestCounts()
                        def affected  = affectedModuleSet()

                        greenReleaseModules().each { mod ->
                            def moduleTotal   = inventory[mod] ?: 0
                            def moduleRun     = affected.contains(mod) ? (executed[mod] ?: 0) : 0
                            def moduleSkipped = affected.contains(mod) ? 0 : moduleTotal
                            moduleDetails[mod] = ['status': affected.contains(mod) ? 'run' : 'skipped', 'run': moduleRun, 'skipped': moduleSkipped]
                            testsRun     += moduleRun
                            testsSkipped += moduleSkipped
                        }

                        env.TESTS_EXECUTED = testsRun.toString()
                        env.TESTS_SKIPPED  = testsSkipped.toString()
                        env.MODULE_DETAILS = groovy.json.JsonOutput.toJson(moduleDetails).replaceAll('"', '\\\\"')

                        echo "Total tests executed: ${env.TESTS_EXECUTED}"
                        echo "Total tests skipped: ${env.TESTS_SKIPPED}"
                        echo "Module Details: ${env.MODULE_DETAILS}"
                    }
                }
            }
        }

        stage('Docker Build') {
            when { expression { !params.DRY_RUN && appAffected() } }
            steps {
                dir(env.WORK_DIR) {
                    script {
                        def dockerStart = System.currentTimeMillis()
                        dir('app') {
                            echo "Building Docker image: ${DOCKER_IMAGE}:${DOCKER_TAG}"
                            sh "docker build -t ${DOCKER_IMAGE}:${DOCKER_TAG} -t ${DOCKER_IMAGE}:latest -t ${DOCKER_IMAGE}:canary ."
                        }
                        env.DOCKER_BUILD_DURATION = ((System.currentTimeMillis() - dockerStart) / 1000.0).toString()
                    }
                }
            }
        }

        stage('Finish Release Monitor') {
            when {
                expression { env.RELEASE_MONITOR_STARTED == 'true' && env.RELEASE_MONITOR_FINISHED != 'true' }
            }
            steps {
                script {
                    finishMonitorSession('release')
                }
            }
        }

        stage('Confirm Deploy Strategy') {
            when {
                expression {
                    !params.DRY_RUN &&
                    appAffected() &&
                    env.URGENT_DEPLOY != 'true' &&
                    (
                        env.BYPASS_GREEN != 'true' ||
                        params.FORCE_FULL_BUILD
                    )
                }
            }
            steps {
                script {
                    def liveStrategy = greenCheck(singleShot: true)

                    if (!liveStrategy) {
                        echo '⚠️ Confirm check returned no strategy. Keeping current strategy.'
                        liveStrategy = env.DEPLOY_STRATEGY ?: 'rolling'
                    }
                    liveStrategy = liveStrategy.toString().toLowerCase().trim()
                    if (!(liveStrategy in ['canary', 'rolling', 'recreate'])) {
                        echo "⚠️ Confirm check returned invalid strategy '${liveStrategy}'. Keeping current strategy."
                        liveStrategy = env.DEPLOY_STRATEGY ?: 'rolling'
                    }

                    def preSelected = env.DEPLOY_STRATEGY ?: ''

                    if (params.FORCE_FULL_BUILD) {
                        env.DEPLOY_STRATEGY = liveStrategy
                        echo "🤖 FORCE FULL BUILD — Green AI selected deployment strategy: ${env.DEPLOY_STRATEGY}"
                    } else if (preSelected && preSelected != liveStrategy) {
                        echo "⚠️ Carbon conditions shifted since scheduling:"
                        echo "   Pre-selected strategy : ${preSelected}"
                        echo "   Live check suggests   : ${liveStrategy}"
                        echo "   Decision              : Keeping pre-selected strategy (${preSelected}) to honour scheduling commitment."
                    } else if (!preSelected) {
                        env.DEPLOY_STRATEGY = liveStrategy
                        echo "🌿 No pre-selected strategy. Using live check result: ${env.DEPLOY_STRATEGY}"
                    } else {
                        echo "✅ Strategy confirmed: ${env.DEPLOY_STRATEGY} (pre-selected and live check agree)"
                    }

                    echo "🌿 Final deploy strategy: ${env.DEPLOY_STRATEGY}"
                }
            }
        }

        stage('Notify Deployment Start') {
            when { expression { !params.DRY_RUN && appAffected() } }
            steps { 
                script {
                    env.DEPLOY_START_MS = System.currentTimeMillis().toString()
                }
                notifyStart() 
            }
        }

        stage('Carbon Snapshot - Before') {
            when { expression { !params.DRY_RUN && appAffected() } }
            steps { carbonSnapshot(phase: 'before') }
        }

        stage('Skip Deploy Monitor') {
            when {
                expression { !deployWorkPlanned() }
            }
            steps {
                script {
                    skipMonitorSession('deploy', deploySkipReason())
                }
            }
        }

        stage('Start Deploy Monitor') {
            when {
                expression { deployWorkPlanned() }
            }
            steps {
                script {
                    startMonitorSession('deploy')
                }
            }
        }

        stage('Deploy Canary') {
            when { expression { !params.DRY_RUN && appAffected() && env.DEPLOY_STRATEGY == 'canary' } }
            steps {
                sh """
                    set -e
                    docker rm -f ${CANARY_CONTAINER} 2>/dev/null || true
                    docker run -d \
                        --name ${CANARY_CONTAINER} \
                        -p 8880:8080 \
                        --label role=canary \
                        --label build=${BUILD_NUMBER} \
                        ${DOCKER_IMAGE}:canary
                    # Wait for the canary container to become healthy.
                    # Spring Boot apps can take 15–30s to start; retry for up to 60s.
                    HEALTH_OK=0
                    for i in \$(seq 1 12); do
                        sleep 5
                        if curl -sf http://host.docker.internal:8880/health > /dev/null 2>&1 || curl -sf http://172.17.0.1:8880/health > /dev/null 2>&1 || curl -sf http://localhost:8880/health > /dev/null 2>&1; then
                            echo "[CANARY] Healthy on /health (attempt \${i})"
                            HEALTH_OK=1
                            break
                        fi
                        if curl -sf http://host.docker.internal:8880/actuator/health > /dev/null 2>&1 || curl -sf http://172.17.0.1:8880/actuator/health > /dev/null 2>&1 || curl -sf http://localhost:8880/actuator/health > /dev/null 2>&1; then
                            echo "[CANARY] Healthy on /actuator/health (attempt \${i})"
                            HEALTH_OK=1
                            break
                        fi
                        echo "[CANARY] Not ready yet... (attempt \${i}/12)"
                    done
                    if [ "\${HEALTH_OK}" -ne 1 ]; then
                        echo "[CANARY] Container failed to become healthy within 60s"
                        docker logs ${CANARY_CONTAINER} --tail 50
                        exit 1
                    fi
                """
                carbonSnapshot(phase: 'canary_live', infraMultiplier: '1.2', canaryWeight: env.CANARY_WEIGHT, note: 'stable_plus_canary_running')
            }
        }

        stage('Observe Canary') {
            when { expression { !params.DRY_RUN && appAffected() && env.DEPLOY_STRATEGY == 'canary' } }
            steps {
                sleep time: "${CANARY_WAIT_SECS}", unit: 'SECONDS'
                sh """
                    set -e
                    curl -sf http://host.docker.internal:8880/health || curl -sf http://172.17.0.1:8880/health || curl -sf http://localhost:8880/health
                    ERROR_COUNT=\$(docker logs --since=60s ${CANARY_CONTAINER} 2>&1 | grep -ci ERROR || true)
                    if [ "\$ERROR_COUNT" -gt 5 ]; then exit 1; fi
                    echo "[CANARY] Error rate acceptable"
                """
            }
        }

        stage('Promote Canary') {
            when { expression { !params.DRY_RUN && appAffected() && env.DEPLOY_STRATEGY == 'canary' } }
            steps {
                dir(env.WORK_DIR) {
                    sh """
                        set -e
                        docker tag ${DOCKER_IMAGE}:canary ${DOCKER_IMAGE}:latest
                        docker-compose down
                        docker-compose up -d
                        docker rm -f ${CANARY_CONTAINER} || true
                        sleep 15
                        docker-compose ps
                    """
                }
                carbonSnapshot(phase: 'promoted', infraMultiplier: '1.0', note: 'canary_removed_stable_updated')
            }
        }

        stage('Deploy Rolling') {
            when { expression { !params.DRY_RUN && appAffected() && env.DEPLOY_STRATEGY == 'rolling' } }
            steps {
                dir(env.WORK_DIR) {
                    sh """
                        set -e
                        CONTAINERS=\$(docker-compose ps -q)
                        TOTAL=\$(echo "\$CONTAINERS" | wc -w)
                        echo "[ROLLING] Found \$TOTAL containers to roll"
                        for CONTAINER in \$CONTAINERS; do
                            NAME=\$(docker inspect --format="{{.Name}}" \$CONTAINER | sed "s#^/##")
                            docker stop --time=10 \$CONTAINER || true
                            docker-compose up -d --no-deps 2>/dev/null || true
                            sleep ${ROLLING_WAIT_SECS}
                            curl -sf http://host.docker.internal:8080/health || curl -sf http://172.17.0.1:8080/health || curl -sf http://localhost:8080/health || exit 1
                            echo "[ROLLING] \$NAME replaced successfully"
                        done
                        docker-compose ps
                    """
                }
                carbonSnapshot(phase: 'during', infraMultiplier: '1.1')
            }
        }

        stage('Deploy Recreate') {
            when { expression { !params.DRY_RUN && appAffected() && env.DEPLOY_STRATEGY == 'recreate' } }
            steps {
                dir(env.WORK_DIR) {
                    sh """
                        set -e
                        docker-compose down
                        docker-compose up -d
                        sleep 20
                        docker-compose ps
                    """
                }
                carbonSnapshot(phase: 'after', infraMultiplier: '1.0', downtimeSeconds: '20')
            }
        }

        stage('Smoke Test (Local)') {
            when { expression { !params.DRY_RUN && appAffected() } }
            steps {
                sh 'curl -sf http://host.docker.internal:8080/health || curl -sf http://172.17.0.1:8080/health || curl -sf http://localhost:8080/health && echo "SMOKE TEST PASSED" || exit 1'
            }
            post {
                always {
                    script {
                        finishMonitorSession('deploy')
                    }
                }
            }
        }
    }

    post {
        success {
            script {
                if (appAffected() && !params.DRY_RUN) {
                    def img = "${DOCKER_IMAGE}:${DOCKER_TAG}"
                    notifyEnd(status: 'SUCCESS', image: img)
                    echo "Deployment complete — Strategy: ${env.DEPLOY_STRATEGY} | Carbon: ${env.CARBON_RATING}"
                }
            }
            dir(env.WORK_DIR) {
                sh "echo ${env.COMMIT_SHA} > .last_built_commit"
            }
            echo "Build SUCCESSFUL — Build #${BUILD_NUMBER}"
        }

        failure {
            script {
                if (env.DEPLOY_STRATEGY == 'canary' && appAffected()) {
                    sh "docker rm -f ${CANARY_CONTAINER} || true"
                }
                if (appAffected() && !params.DRY_RUN) {
                    notifyEnd(status: 'FAILURE')
                }
            }
            echo "Build FAILED — Build #${BUILD_NUMBER}"
        }

        always {
            script {
                finishMonitorSession('release')
                finishMonitorSession('deploy')
            }

            dir(env.WORK_DIR) {
                script {
                    def totalDuration = (System.currentTimeMillis() - env.PIPELINE_START.toLong()) / 1000.0
                    def currentStatus = currentBuild.currentResult ?: 'UNKNOWN'
                    if (env.IS_RESCHEDULED == 'true') currentStatus = 'RESCHEDULED'

                    if (env.DEPLOY_START_MS) {
                        env.DEPLOY_DURATION = ((System.currentTimeMillis() - env.DEPLOY_START_MS.toLong()) / 1000.0).toString()
                    }

                    def cleanCommitMsg = (env.COMMIT_MSG ?: '').replaceAll('"', '\\\\"')
                    def jsonPayload = """{
                        "job_name": "${env.JOB_NAME}",
                        "build_number": "${env.BUILD_NUMBER}",
                        "pipeline_type": "optimized_build_and_deploy",
                        "commit_sha": "${env.COMMIT_SHA ?: ''}",
                        "commit_message": "${cleanCommitMsg}",
                        "status": "${currentStatus}",
                        "total_duration_s": ${totalDuration},
                        "build_duration_s": ${env.BUILD_DURATION ?: 'null'},
                        "test_duration_s": ${env.TEST_DURATION ?: 'null'},
                        "docker_duration_s": ${env.DOCKER_BUILD_DURATION ?: 'null'},
                        "deploy_duration_s": ${env.DEPLOY_DURATION ?: 'null'},
                        "optimizer_duration_s": ${env.OPTIMIZER_DURATION ?: 'null'},
                        "modules_built": "${env.AFFECTED_MODULES ?: ''}",
                        "modules_tested": "${env.AFFECTED_MODULES ?: ''}",
                        "tests_executed": ${env.TESTS_EXECUTED ?: 0},
                        "tests_skipped": ${env.TESTS_SKIPPED ?: 0},
                        "module_details": "${env.MODULE_DETAILS ?: ''}",
                        "build_command": "${(env.MAVEN_BUILD_COMMANDS ?: '').replaceAll('"', '\\\\"')}",
                        "test_command": "${(env.MAVEN_TEST_COMMANDS ?: '').replaceAll('"', '\\\\"')}",
                        "carbon_intensity": ${env.CARBON_INTENSITY ?: 'null'},
                        "green_probability": ${env.GREEN_PROBABILITY ?: 'null'},
                        "scheduling_action": "${env.SCHEDULING_ACTION ?: ''}",
                        "scheduling_engine": "${env.SCHEDULING_ENGINE ?: ''}",
                        "carbon_history": ${env.CARBON_HISTORY ?: '[]'},
                        "carbon_forecast": ${env.CARBON_FORECAST ?: '[]'},
                        "deploy_strategy": "${env.DEPLOY_STRATEGY ?: ''}",
                        "combined_confidence": ${env.COMBINED_CONFIDENCE ?: 'null'},
                        "ml_green_probability": ${env.ML_GREEN_PROBABILITY ?: 'null'},
                        "ai_confidence": ${env.AI_CONFIDENCE ?: 'null'},
                        "both_schedulers_agree": "${env.BOTH_SCHEDULERS_AGREE ?: ''}",
                        "scheduling_reason": "${(env.SCHEDULING_REASON ?: '').replaceAll('"', '\\\\"')}"
                    }"""

                    writeFile file: 'dashboard_payload.json', text: jsonPayload

                    sh """
                        curl -s -X POST ${DASHBOARD_URL}/api/builds \
                            -H "Content-Type: application/json" \
                            -d @dashboard_payload.json || \
                        curl -s -X POST http://localhost:5005/api/builds \
                            -H "Content-Type: application/json" \
                            -d @dashboard_payload.json || \
                        curl -s -X POST http://127.0.0.1:5005/api/builds \
                            -H "Content-Type: application/json" \
                            -d @dashboard_payload.json || \
                        curl -s -X POST http://172.17.0.1:5005/api/builds \
                            -H "Content-Type: application/json" \
                            -d @dashboard_payload.json || \
                        echo "Failed to send metrics to dashboard."
                    """
                }

                sh "docker rmi ${DOCKER_IMAGE}:${DOCKER_TAG} || true"
                sh "docker rmi ${DOCKER_IMAGE}:canary || true"
                sh "docker image prune -f || true"
            }
        }
    }
}
