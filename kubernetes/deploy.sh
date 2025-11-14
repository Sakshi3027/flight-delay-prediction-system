#!/bin/bash

# Flight Delay Prediction System - Kubernetes Deployment Script
# This script automates the deployment of all components to Kubernetes

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
NAMESPACE="flight-delay-system"
TIMEOUT=300

# Functions
print_header() {
    echo -e "${GREEN}======================================${NC}"
    echo -e "${GREEN}$1${NC}"
    echo -e "${GREEN}======================================${NC}"
}

print_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_prerequisites() {
    print_header "Checking Prerequisites"
    
    # Check kubectl
    if ! command -v kubectl &> /dev/null; then
        print_error "kubectl is not installed"
        exit 1
    fi
    print_success "kubectl is installed"
    
    # Check cluster connection
    if ! kubectl cluster-info &> /dev/null; then
        print_error "Cannot connect to Kubernetes cluster"
        exit 1
    fi
    print_success "Connected to Kubernetes cluster"
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed"
        exit 1
    fi
    print_success "Docker is installed"
}

create_namespace() {
    print_header "Creating Namespace"
    
    if kubectl get namespace $NAMESPACE &> /dev/null; then
        print_info "Namespace $NAMESPACE already exists"
    else
        kubectl apply -f namespace.yaml
        print_success "Namespace $NAMESPACE created"
    fi
}

deploy_zookeeper() {
    print_header "Deploying Zookeeper"
    
    kubectl apply -f zookeeper-deployment.yaml
    print_info "Waiting for Zookeeper to be ready..."
    
    kubectl wait --for=condition=ready pod -l app=zookeeper \
        -n $NAMESPACE --timeout=${TIMEOUT}s
    
    print_success "Zookeeper is ready"
}

deploy_kafka() {
    print_header "Deploying Kafka"
    
    kubectl apply -f kafka-deployment.yaml
    print_info "Waiting for Kafka to be ready..."
    
    kubectl wait --for=condition=ready pod -l app=kafka \
        -n $NAMESPACE --timeout=${TIMEOUT}s
    
    print_success "Kafka is ready"
}

deploy_spark_master() {
    print_header "Deploying Spark Master"
    
    kubectl apply -f spark-master-deployment.yaml
    print_info "Waiting for Spark Master to be ready..."
    
    kubectl wait --for=condition=ready pod -l app=spark-master \
        -n $NAMESPACE --timeout=${TIMEOUT}s
    
    print_success "Spark Master is ready"
}

deploy_spark_workers() {
    print_header "Deploying Spark Workers"
    
    kubectl apply -f spark-worker-deployment.yaml
    print_info "Waiting for Spark Workers to be ready..."
    
    kubectl wait --for=condition=ready pod -l app=spark-worker \
        -n $NAMESPACE --timeout=${TIMEOUT}s
    
    print_success "Spark Workers are ready"
}

deploy_dashboard() {
    print_header "Deploying Dashboard"
    
    kubectl apply -f dashboard-deployment.yaml
    print_info "Waiting for Dashboard to be ready..."
    
    kubectl wait --for=condition=ready pod -l app=dashboard \
        -n $NAMESPACE --timeout=${TIMEOUT}s
    
    print_success "Dashboard is ready"
}

deploy_producer() {
    print_header "Deploying Data Producer"
    
    kubectl apply -f producer-deployment.yaml
    print_info "Waiting for Producer to be ready..."
    
    sleep 10  # Give it time to start
    print_success "Data Producer deployed"
}

show_status() {
    print_header "Deployment Status"
    
    echo ""
    print_info "All Pods:"
    kubectl get pods -n $NAMESPACE
    
    echo ""
    print_info "All Services:"
    kubectl get svc -n $NAMESPACE
    
    echo ""
    print_info "Horizontal Pod Autoscalers:"
    kubectl get hpa -n $NAMESPACE
}

get_access_urls() {
    print_header "Access Information"
    
    echo ""
    print_info "Dashboard Service:"
    kubectl get svc dashboard -n $NAMESPACE
    
    DASHBOARD_IP=$(kubectl get svc dashboard -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "pending")
    
    if [ "$DASHBOARD_IP" != "pending" ]; then
        echo ""
        print_success "Dashboard URL: http://${DASHBOARD_IP}:5000"
    else
        echo ""
        print_info "LoadBalancer IP is pending. Use port-forward to access:"
        print_info "kubectl port-forward svc/dashboard 5000:5000 -n $NAMESPACE"
        print_info "Then visit: http://localhost:5000"
    fi
    
    echo ""
    print_info "Spark Master UI:"
    SPARK_IP=$(kubectl get svc spark-master -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "pending")
    
    if [ "$SPARK_IP" != "pending" ]; then
        print_success "Spark Master URL: http://${SPARK_IP}:8080"
    else
        print_info "Use port-forward to access Spark Master:"
        print_info "kubectl port-forward svc/spark-master 8081:8080 -n $NAMESPACE"
        print_info "Then visit: http://localhost:8081"
    fi
}

# Main deployment flow
main() {
    print_header "Flight Delay Prediction System - K8s Deployment"
    
    check_prerequisites
    create_namespace
    deploy_zookeeper
    deploy_kafka
    deploy_spark_master
    deploy_spark_workers
    deploy_dashboard
    deploy_producer
    show_status
    get_access_urls
    
    echo ""
    print_header "Deployment Complete!"
    print_success "All components have been deployed successfully"
    echo ""
    print_info "To view logs:"
    print_info "kubectl logs -f deployment/dashboard -n $NAMESPACE"
    echo ""
    print_info "To cleanup:"
    print_info "kubectl delete namespace $NAMESPACE"
    echo ""
}

# Run main function
main