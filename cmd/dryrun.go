/*
 * Copyright The Microcks Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/microcks/microcks-cli/pkg/connectors"
	microcks "microcks.io/testcontainers-go"
)

// dryRunSupportedRunners defines the runner types supported in dry-run mode.
// Runners requiring MicrocksContainersEnsemble (POSTMAN, ASYNC_API_SCHEMA)
// are excluded as they need Kafka and the async-minion sidecar.
var dryRunSupportedRunners = map[string]bool{
	"HTTP":            true,
	"SOAP_HTTP":       true,
	"SOAP_UI":         true,
	"OPEN_API_SCHEMA": true,
	"GRPC_PROTOBUF":   true,
	"GRAPHQL_SCHEMA":  true,
}

// dryRunConfig holds all parameters needed to execute a dry-run test.
type dryRunConfig struct {
	serviceRef          string
	testEndpoint        string
	runnerType          string
	artifactPath        string
	waitForMilliseconds int64
	secretName          string
	filteredOperations  string
	operationsHeaders   string
	oAuth2Context       string
}

// runDryTest starts an ephemeral Microcks container via Testcontainers,
// imports the specified artifact, runs the contract test, reports results,
// and tears down the container.
func runDryTest(cfg dryRunConfig) error {
	ctx := context.Background()

	if !dryRunSupportedRunners[cfg.runnerType] {
		return fmt.Errorf("--dry-run does not support %s runner (requires MicrocksContainersEnsemble)", cfg.runnerType)
	}

	if cfg.artifactPath == "" {
		return fmt.Errorf("--artifact flag is required when using --dry-run")
	}

	if _, err := os.Stat(cfg.artifactPath); os.IsNotExist(err) {
		return fmt.Errorf("artifact file not found: %s", cfg.artifactPath)
	}

	fmt.Println("Starting ephemeral Microcks container...")
	startTime := time.Now()

	// Start the Microcks uber container with the artifact imported on startup.
	// The uber image has Keycloak disabled, so no authentication is required.
	mc, err := microcks.Run(ctx, microcks.DefaultImage,
		microcks.WithMainArtifact(cfg.artifactPath),
	)
	if err != nil {
		return fmt.Errorf("failed to start Microcks container: %w", err)
	}
	defer func() {
		fmt.Println("Tearing down ephemeral Microcks container...")
		if termErr := mc.Terminate(ctx); termErr != nil {
			fmt.Printf("Warning: failed to terminate container: %s\n", termErr)
		}
	}()

	httpEndpoint, err := mc.HttpEndpoint(ctx)
	if err != nil {
		return fmt.Errorf("failed to get Microcks endpoint: %w", err)
	}
	fmt.Printf("Microcks container started at %s (took %s)\n",
		httpEndpoint, time.Since(startTime).Round(time.Millisecond))

	// Create a MicrocksClient pointing at the ephemeral container.
	// The uber image runs without Keycloak, so we use an unauthenticated token.
	client := connectors.NewMicrocksClient(httpEndpoint)
	client.SetOAuthToken("unauthenticated-token")

	// Create the test on the ephemeral instance.
	fmt.Printf("Running test for \"%s\" against %s using %s runner...\n",
		cfg.serviceRef, cfg.testEndpoint, cfg.runnerType)

	testResultID, err := client.CreateTestResult(
		cfg.serviceRef, cfg.testEndpoint, cfg.runnerType,
		cfg.secretName, cfg.waitForMilliseconds,
		cfg.filteredOperations, cfg.operationsHeaders, cfg.oAuth2Context,
	)
	if err != nil {
		return fmt.Errorf("failed to create test: %w", err)
	}

	// Poll for test completion using the same pattern as the regular test command.
	time.Sleep(1 * time.Second)

	future := nowInMilliseconds() + cfg.waitForMilliseconds + 10000
	var success bool

	for nowInMilliseconds() < future {
		testResultSummary, err := client.GetTestResult(testResultID)
		if err != nil {
			return fmt.Errorf("failed to get test result: %w", err)
		}

		success = testResultSummary.Success
		inProgress := testResultSummary.InProgress
		fmt.Printf("MicrocksClient got status for test \"%s\" - success: %s, inProgress: %s\n",
			testResultID, fmt.Sprint(success), fmt.Sprint(inProgress))

		if !inProgress {
			break
		}

		fmt.Println("MicrocksTester waiting for 2 seconds before checking again or exiting.")
		time.Sleep(2 * time.Second)
	}

	fmt.Printf("Full TestResult details are available here: %s/#/tests/%s\n",
		httpEndpoint, testResultID)

	if !success {
		return fmt.Errorf("test \"%s\" failed", testResultID)
	}

	return nil
}
