provider "aws" {
  region = var.aws_region
}

# 1. Networking (Minimal VPC Setup)
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags = { Name = "${var.environment}-vpc" }
}

resource "aws_internet_gateway" "gw" {
  vpc_id = aws_vpc.main.id
  tags = { Name = "${var.environment}-igw" }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  map_public_ip_on_launch = true
  availability_zone       = "${var.aws_region}a"
  tags = { Name = "${var.environment}-public-subnet" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.gw.id
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# 2. Security Group (Lock it down)
resource "aws_security_group" "metal_sg" {
  name        = "${var.environment}-metal-sg"
  description = "Security group for Firecracker Bare Metal nodes"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "SSH Access (Restricted in prod)"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "Allow outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# 3. Data Source for latest Ubuntu AMI
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

# 4. Spot Launch Template for Execution Plane
resource "aws_launch_template" "metal_worker" {
  name_prefix   = "${var.environment}-metal-worker-"
  image_id      = data.aws_ami.ubuntu.id
  instance_type = var.metal_instance_type

  network_interfaces {
    associate_public_ip_address = true
    security_groups             = [aws_security_group.metal_sg.id]
  }

  # Cloud-init script to install Firecracker and start Orchestrator
  user_data = base64encode(templatefile("${path.module}/user_data.sh", {
    DATABASE_URL     = var.database_url
    REDPANDA_BROKERS = var.redpanda_brokers
  }))

  # Request spot instances to save 70% on bare metal
  instance_market_options {
    market_type = "spot"
  }

  lifecycle {
    create_before_destroy = true
  }
}

# 5. Auto Scaling Group (Horizontal Scalability)
resource "aws_autoscaling_group" "metal_asg" {
  name                = "${var.environment}-metal-asg"
  vpc_zone_identifier = [aws_subnet.public.id]
  min_size            = 0
  max_size            = 10
  desired_capacity    = 1 # Adjust based on queue length

  launch_template {
    id      = aws_launch_template.metal_worker.id
    version = "$Latest"
  }

  tag {
    key                 = "Name"
    value               = "${var.environment}-worker-node"
    propagate_at_launch = true
  }
}
